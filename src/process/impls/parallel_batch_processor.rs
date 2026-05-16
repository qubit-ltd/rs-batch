/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    num::NonZeroUsize,
    sync::Arc,
    thread,
    time::Duration,
};

use qubit_function::{
    ArcConsumer,
    Consumer,
};
use qubit_progress::{
    Progress,
    reporter::ProgressReporter,
};

use crate::process::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessState,
    BatchProcessor,
};
use crate::utils::run_scoped_parallel;

use super::parallel_batch_processor_builder::ParallelBatchProcessorBuilder;

/// Processes batch items with sequential fallback and scoped standard threads.
///
/// The processor stores the supplied consumer as an [`ArcConsumer`] so every
/// worker can share it safely. By default, small batches run sequentially to
/// avoid thread setup overhead. Larger batches use scoped worker threads for
/// each [`BatchProcessor::process`] call, therefore input items may borrow data
/// from the caller as long as they are [`Send`]. Running progress is reported
/// between items on the sequential path and from a scoped reporter thread on
/// the parallel path.
///
/// # Type Parameters
///
/// * `Item` - Item type consumed by the stored consumer.
///
/// ```rust
/// use std::{
///     num::NonZeroUsize,
///     sync::{
///         Arc,
///         atomic::{
///             AtomicUsize,
///             Ordering,
///         },
///     },
/// };
///
/// use qubit_batch::{
///     BatchProcessor,
///     ParallelBatchProcessor,
/// };
///
/// let total = Arc::new(AtomicUsize::new(0));
/// let total_for_consumer = Arc::clone(&total);
/// let mut processor = ParallelBatchProcessor::builder(move |item: &usize| {
///     total_for_consumer.fetch_add(*item, Ordering::Relaxed);
/// })
/// .thread_count(NonZeroUsize::new(2).expect("thread count should be non-zero"))
/// .sequential_threshold(0)
/// .build();
///
/// let result = processor
///     .process([1, 2, 3])
///     .expect("array length should be exact");
///
/// assert!(result.is_success());
/// assert_eq!(total.load(Ordering::Relaxed), 6);
/// ```
pub struct ParallelBatchProcessor<Item> {
    /// Consumer shared by all scoped workers.
    pub(crate) consumer: ArcConsumer<Item>,
    /// Fixed worker-thread count used by each processing call.
    pub(crate) thread_count: NonZeroUsize,
    /// Maximum batch size that still uses sequential processing.
    pub(crate) sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn ProgressReporter>,
}

impl<Item> ParallelBatchProcessor<Item> {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Default maximum batch size that still uses sequential processing.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = 100;

    /// Creates a parallel consumer-backed batch processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Thread-safe consumer invoked once for each accepted item.
    ///
    /// # Returns
    ///
    /// A processor storing `consumer` as an [`ArcConsumer`] and using
    /// [`Self::default_thread_count`] workers.
    #[inline]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + Send + Sync + 'static,
    {
        Self::builder(consumer).build()
    }

    /// Creates a builder for configuring a parallel consumer-backed processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Thread-safe consumer invoked once for each accepted item.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[inline]
    pub fn builder<C>(consumer: C) -> ParallelBatchProcessorBuilder<Item>
    where
        C: Consumer<Item> + Send + Sync + 'static,
    {
        ParallelBatchProcessorBuilder::new(consumer)
    }

    /// Returns the default worker-thread count.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[inline]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }

    /// Returns the configured worker-thread count.
    ///
    /// # Returns
    ///
    /// The maximum number of scoped worker threads used for one batch.
    #[inline]
    pub const fn thread_count(&self) -> usize {
        self.thread_count.get()
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum item count that still runs sequentially.
    #[inline]
    pub const fn sequential_threshold(&self) -> usize {
        self.sequential_threshold
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between due-based running progress callbacks.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the configured progress reporter.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.reporter
    }

    /// Returns the stored consumer.
    ///
    /// # Returns
    ///
    /// A shared reference to the arc-backed consumer.
    #[inline]
    pub const fn consumer(&self) -> &ArcConsumer<Item> {
        &self.consumer
    }

    /// Consumes this processor and returns the stored consumer.
    ///
    /// # Returns
    ///
    /// The arc-backed consumer used by this processor.
    #[inline]
    pub fn into_consumer(self) -> ArcConsumer<Item> {
        self.consumer
    }
}

impl<Item> BatchProcessor<Item> for ParallelBatchProcessor<Item>
where
    Item: Send,
{
    type Error = BatchProcessError;

    /// Processes items sequentially for small batches or on scoped workers.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared number of items expected from `items`.
    ///
    /// # Returns
    ///
    /// A result with completed and processed counts equal to the number of
    /// consumer calls when the input source yields exactly `count` items.
    ///
    /// # Errors
    ///
    /// Returns [`BatchProcessError::CountShortfall`] when the source ends before
    /// `count`, or [`BatchProcessError::CountExceeded`] when the source yields an
    /// extra item. Extra items are observed but not passed to the consumer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stored consumer from the caller thread
    /// or a worker thread, or by the configured progress reporter.
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let state = Arc::new(BatchProcessState::new(count));
        let mut progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_started(state.progress_counters());

        if count > 0 {
            if count <= self.sequential_threshold {
                self.process_sequential(items, count, state.as_ref(), &mut progress);
            } else {
                self.process_parallel_non_empty(items, count, Arc::clone(&state), &progress);
            }
        } else if items.into_iter().next().is_some() {
            state.record_item_observed();
        }

        if state.observed_count() < count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.to_direct_result(failed.elapsed());
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else if state.observed_count() > count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.to_direct_result(failed.elapsed());
            Err(BatchProcessError::CountExceeded {
                expected: count,
                observed_at_least: state.observed_count(),
                result,
            })
        } else {
            let finished = progress.report_finished(state.progress_counters());
            let result = state.to_direct_result(finished.elapsed());
            Ok(result)
        }
    }
}

impl<Item> ParallelBatchProcessor<Item>
where
    Item: Send,
{
    /// Processes a declared batch on the caller thread.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared item count.
    /// * `state` - Processing state updated by this method.
    /// * `progress` - Progress run used for between-item running callbacks.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised while invoking the stored consumer.
    fn process_sequential<I>(
        &self,
        items: I,
        count: usize,
        state: &BatchProcessState,
        progress: &mut Progress<'_>,
    ) where
        I: IntoIterator<Item = Item>,
    {
        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                break;
            }
            state.record_item_started();
            self.consumer.accept(&item);
            state.record_item_processed();
            let _ = progress.report_running_if_due(state.progress_counters());
        }
    }

    /// Processes a non-empty declared batch through scoped workers.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared item count.
    /// * `state` - Shared processing state updated by producer and workers.
    /// * `progress` - Progress run used to spawn the running reporter.
    ///
    /// # Panics
    ///
    /// Propagates any worker panic raised while invoking the stored consumer.
    fn process_parallel_non_empty<I>(
        &self,
        items: I,
        count: usize,
        state: Arc<BatchProcessState>,
        progress: &Progress<'_>,
    ) where
        I: IntoIterator<Item = Item>,
    {
        thread::scope(|scope| {
            let reporter_state = Arc::clone(&state);
            let running_progress =
                progress.spawn_running_reporter(scope, move || reporter_state.progress_counters());
            let running_point_handle = running_progress.point_handle();

            let worker_count = self.thread_count.get().min(count);
            let observer_state = Arc::clone(&state);
            let worker_state = Arc::clone(&state);
            let consumer = self.consumer.clone();
            run_scoped_parallel(
                items,
                count,
                worker_count,
                move || observer_state.record_item_observed(),
                move |_index, item| {
                    worker_state.record_item_started();
                    consumer.accept(&item);
                    worker_state.record_item_processed();
                    running_point_handle.report();
                },
            );
            running_progress.stop_and_join();
        });
    }
}
