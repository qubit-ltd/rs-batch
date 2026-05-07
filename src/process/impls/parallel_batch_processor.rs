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
    panic::resume_unwind,
    sync::Arc,
    thread,
    time::{
        Duration,
        Instant,
    },
};

use qubit_function::{
    ArcConsumer,
    Consumer,
};
use qubit_progress::{
    Progress,
    RunningProgressLoop,
    model::ProgressPhase,
    reporter::{
        NoOpProgressReporter,
        ProgressReporter,
    },
};

use crate::process::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessState,
    BatchProcessor,
};
use crate::utils::run_scoped_parallel;

/// Processes batch items in parallel on scoped standard threads.
///
/// The processor stores the supplied consumer as an [`ArcConsumer`] so every
/// worker can share it safely. Worker threads are scoped to each
/// [`BatchProcessor::process`] call, therefore input items may borrow data from
/// the caller as long as they are [`Send`]. Running progress is reported from a
/// scoped reporter thread while workers update shared counters.
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
/// let mut processor = ParallelBatchProcessor::new(move |item: &usize| {
///     total_for_consumer.fetch_add(*item, Ordering::Relaxed);
/// })
/// .with_thread_count(NonZeroUsize::new(2).expect("thread count should be non-zero"));
///
/// let result = processor
///     .process([1, 2, 3], 3)
///     .expect("iterator should yield exactly three items");
///
/// assert!(result.is_success());
/// assert_eq!(total.load(Ordering::Relaxed), 6);
/// ```
pub struct ParallelBatchProcessor<Item> {
    /// Consumer shared by all scoped workers.
    consumer: ArcConsumer<Item>,
    /// Fixed worker-thread count used by each processing call.
    thread_count: NonZeroUsize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl<Item> ParallelBatchProcessor<Item> {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

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
        Self {
            consumer: consumer.into_arc(),
            thread_count: NonZeroUsize::new(Self::default_thread_count())
                .expect("default parallel processor thread count should be non-zero"),
            report_interval: Self::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
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

    /// Returns a copy configured with a fixed worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Non-zero number of scoped worker threads.
    ///
    /// # Returns
    ///
    /// This processor configured to use `thread_count` workers per call.
    #[inline]
    pub const fn with_thread_count(mut self, thread_count: NonZeroUsize) -> Self {
        self.thread_count = thread_count;
        self
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter<R>(self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.with_reporter_arc(Arc::new(reporter))
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter_arc(self, reporter: Arc<dyn ProgressReporter>) -> Self {
        Self { reporter, ..self }
    }

    /// Returns a copy configured with the supplied progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports on worker completion signals
    ///   without periodic polling.
    ///
    /// # Returns
    ///
    /// This processor configured with `report_interval`.
    #[inline]
    pub fn with_report_interval(self, report_interval: Duration) -> Self {
        Self {
            report_interval,
            ..self
        }
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

    /// Processes items on fixed-width scoped standard threads.
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
    /// Propagates any panic raised by the stored consumer from a worker thread,
    /// or by the configured progress reporter.
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let state = Arc::new(BatchProcessState::new(count));
        let progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            state.progress_counters(),
            Duration::ZERO,
        );
        let start = progress.started_at();

        if count > 0 {
            self.process_non_empty(items, count, Arc::clone(&state), start);
        } else if items.into_iter().next().is_some() {
            state.record_item_observed();
        }

        let result = state.to_direct_result(start.elapsed());
        if state.observed_count() < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                state.progress_counters(),
                result.elapsed(),
            );
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else if state.observed_count() > count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                state.progress_counters(),
                result.elapsed(),
            );
            Err(BatchProcessError::CountExceeded {
                expected: count,
                observed_at_least: state.observed_count(),
                result,
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                state.progress_counters(),
                result.elapsed(),
            );
            Ok(result)
        }
    }
}

impl<Item> ParallelBatchProcessor<Item>
where
    Item: Send,
{
    /// Processes a non-empty declared batch through scoped workers.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared item count.
    /// * `state` - Shared processing state updated by producer and workers.
    ///
    /// # Panics
    ///
    /// Propagates any worker panic raised while invoking the stored consumer.
    fn process_non_empty<I>(
        &self,
        items: I,
        count: usize,
        state: Arc<BatchProcessState>,
        start: Instant,
    ) where
        I: IntoIterator<Item = Item>,
    {
        thread::scope(|scope| {
            let (progress_loop, progress_notifier) = RunningProgressLoop::channel();
            let progress_handle = {
                let progress_reporter = Arc::clone(&self.reporter);
                let reporter_state = Arc::clone(&state);
                let report_interval = self.report_interval;
                scope.spawn(move || {
                    let progress =
                        Progress::from_start(progress_reporter.as_ref(), report_interval, start);
                    progress_loop.run(progress, || reporter_state.progress_counters());
                })
            };

            let worker_count = self.thread_count.get().min(count);
            let observer_state = Arc::clone(&state);
            let worker_state = Arc::clone(&state);
            let consumer = self.consumer.clone();
            let worker_progress_notifier = progress_notifier.clone();
            let report_on_worker_completion = self.report_interval.is_zero();
            run_scoped_parallel(
                items,
                count,
                worker_count,
                move || observer_state.record_item_observed(),
                move |_index, item| {
                    worker_state.record_item_started();
                    consumer.accept(&item);
                    worker_state.record_item_processed();
                    if report_on_worker_completion {
                        worker_progress_notifier.running_point();
                    }
                },
            );
            progress_notifier.stop();
            if let Err(payload) = progress_handle.join() {
                resume_unwind(payload);
            }
        });
    }
}
