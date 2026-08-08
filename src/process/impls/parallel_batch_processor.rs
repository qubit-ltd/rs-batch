// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_function::ArcConsumer;
use qubit_function::Consumer;
use qubit_progress::AutoReporterError;
use qubit_progress::AutoReporterStatus;
use qubit_progress::EmissionError;
use qubit_progress::Metric;
use qubit_progress::Progress;
use qubit_progress::ProgressNotifier;
use qubit_progress::reporter::Reporter;

use super::parallel_batch_processor_builder::ParallelBatchProcessorBuilder;
use crate::ProgressFailure;
use crate::process::BatchProcessError;
use crate::process::BatchProcessResult;
use crate::process::BatchProcessState;
use crate::process::BatchProcessor;
use crate::process::PROCESS_PROGRESS_METRIC_ID;
use crate::process::PROCESS_PROGRESS_METRIC_NAME;
use crate::utils::run_scoped_parallel;

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
/// .thread_count(2)
/// .sequential_threshold(0)
/// .build()
/// .expect("parallel processor configuration should be valid");
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
    pub(crate) reporter: Arc<dyn Reporter>,
}

impl<Item> ParallelBatchProcessor<Item> {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = crate::constants::DEFAULT_REPORT_INTERVAL;

    /// Default maximum batch size that still uses sequential processing.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = crate::constants::DEFAULT_SEQUENTIAL_THRESHOLD;

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
        Self::builder(consumer)
            .build()
            .expect("default parallel batch processor should build")
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
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
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
    /// Returns [`BatchProcessError::CountShortfall`] when the source ends
    /// before `count`, or [`BatchProcessError::CountExceeded`] when the
    /// source yields an extra item. Extra items are observed but not passed
    /// to the consumer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stored consumer from the caller
    /// thread or a worker thread, or by the configured progress reporter.
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let mut progress =
            match Progress::builder_arc(Arc::clone(&self.reporter))
                .interval(self.report_interval)
                .metric(
                    Metric::new(
                        PROCESS_PROGRESS_METRIC_ID,
                        PROCESS_PROGRESS_METRIC_NAME,
                    )
                    .total(count as u64),
                )
                .start()
            {
                Ok(progress) => progress,
                Err(source) => {
                    return Err(BatchProcessError::ProgressReport {
                        source: Box::new(ProgressFailure::from(source)),
                        result: BatchProcessResult::builder(count)
                            .elapsed(Duration::ZERO)
                            .build()
                            .expect("empty batch process result must be valid"),
                    });
                }
            };
        let metric = progress
            .metric(PROCESS_PROGRESS_METRIC_ID)
            .expect("configured process metric must exist");
        let state = Arc::new(BatchProcessState::new(count, metric));

        let running_result: Result<(), ProgressFailure> = if count > 0 {
            if count <= self.sequential_threshold
                || self.thread_count.get() <= 1
            {
                self.process_sequential(
                    items,
                    count,
                    state.as_ref(),
                    &mut progress,
                )
                .map_err(ProgressFailure::from)
            } else {
                self.process_parallel_non_empty(
                    items,
                    count,
                    Arc::clone(&state),
                    &mut progress,
                )
                .map_err(ProgressFailure::from)
            }
        } else if items.into_iter().next().is_some() {
            state.record_item_observed();
            Ok(())
        } else {
            Ok(())
        };

        if let Err(source) = running_result {
            return Err(BatchProcessError::ProgressReport {
                source: Box::new(source),
                result: state.to_direct_result(progress.elapsed()),
            });
        }

        if state.observed_count() < count {
            let (elapsed, report_error) = match progress.fail() {
                Ok(elapsed) => (elapsed, None),
                Err(source) => {
                    let elapsed = source.elapsed();
                    (elapsed, Some(ProgressFailure::from(source)))
                }
            };
            let result = state.to_direct_result(elapsed);
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
                report_error,
            })
        } else if state.observed_count() > count {
            let (elapsed, report_error) = match progress.fail() {
                Ok(elapsed) => (elapsed, None),
                Err(source) => {
                    let elapsed = source.elapsed();
                    (elapsed, Some(ProgressFailure::from(source)))
                }
            };
            let result = state.to_direct_result(elapsed);
            Err(BatchProcessError::CountExceeded {
                expected: count,
                observed_at_least: state.observed_count(),
                result,
                report_error,
            })
        } else {
            let finished = match progress.finish() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let elapsed = source.elapsed();
                    let failure = ProgressFailure::from_finish_error(source);
                    return Err(BatchProcessError::ProgressReport {
                        source: Box::new(failure),
                        result: state.to_direct_result(elapsed),
                    });
                }
            };
            let result = state.to_direct_result(finished);
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
    ) -> Result<(), EmissionError>
    where
        I: IntoIterator<Item = Item>,
    {
        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                break;
            }
            state
                .record_item_started()
                .expect("batch progress state transition must be valid");
            self.consumer.accept(&item);
            state
                .record_item_processed()
                .expect("batch progress state transition must be valid");
            progress.report_if_due()?;
        }
        Ok(())
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
        progress: &mut Progress<'_>,
    ) -> Result<(), AutoReporterError>
    where
        I: IntoIterator<Item = Item>,
    {
        thread::scope(|scope| {
            let running_progress = progress.spawn_auto_reporter(scope);
            let running_point_handle: ProgressNotifier =
                running_progress.notifier();
            let running_status: AutoReporterStatus = running_progress.status();

            let worker_count = self.thread_count.get().min(count);
            let observer_state = Arc::clone(&state);
            let worker_state = Arc::clone(&state);
            let consumer = self.consumer.clone();
            run_scoped_parallel(
                items,
                count,
                worker_count,
                move || observer_state.record_item_observed(),
                move || running_status.is_failed(),
                move |_index, item| {
                    worker_state.record_item_started().expect(
                        "batch progress state transition must be valid",
                    );
                    consumer.accept(&item);
                    worker_state.record_item_processed().expect(
                        "batch progress state transition must be valid",
                    );
                    running_point_handle.notify();
                },
            );
            running_progress.stop()
        })
    }
}
