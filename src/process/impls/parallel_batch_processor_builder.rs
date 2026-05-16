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
    time::Duration,
};

use qubit_function::{
    ArcConsumer,
    Consumer,
};
use qubit_progress::reporter::{
    NoOpProgressReporter,
    ProgressReporter,
};

use super::ParallelBatchProcessor;
use super::ParallelBatchProcessorBuildError;

/// Builder for [`ParallelBatchProcessor`].
///
/// Use the builder when the default worker count, sequential fallback
/// threshold, progress interval, or reporter should be customized.
///
/// ```rust
/// use qubit_batch::ParallelBatchProcessor;
///
/// let processor = ParallelBatchProcessor::builder(|_item: &i32| {})
///     .thread_count(2)
///     .sequential_threshold(0)
///     .build()
///     .expect("parallel processor configuration should be valid");
///
/// assert_eq!(processor.thread_count(), 2);
/// assert_eq!(processor.sequential_threshold(), 0);
/// ```
pub struct ParallelBatchProcessorBuilder<Item> {
    /// Consumer shared by all scoped workers.
    consumer: ArcConsumer<Item>,
    /// Fixed worker-thread count used by each processing call.
    thread_count: usize,
    /// Maximum batch size that still uses sequential processing.
    sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl<Item> ParallelBatchProcessorBuilder<Item> {
    /// Creates a builder from a thread-safe consumer.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Thread-safe consumer invoked once for each accepted item.
    ///
    /// # Returns
    ///
    /// A builder initialized with default parallel processor settings.
    #[inline]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + Send + Sync + 'static,
    {
        Self {
            consumer: consumer.into_arc(),
            thread_count: ParallelBatchProcessor::<Item>::default_thread_count(),
            sequential_threshold: ParallelBatchProcessor::<Item>::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: ParallelBatchProcessor::<Item>::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }

    /// Sets the worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Number of scoped worker threads to use.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn thread_count(mut self, thread_count: usize) -> Self {
        self.thread_count = thread_count;
        self
    }

    /// Sets the sequential fallback threshold.
    ///
    /// # Parameters
    ///
    /// * `sequential_threshold` - Maximum declared item count that still runs
    ///   on the caller thread. Use `0` when every non-empty batch should use
    ///   scoped workers.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn sequential_threshold(mut self, sequential_threshold: usize) -> Self {
        self.sequential_threshold = sequential_threshold;
        self
    }

    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every sequential between-item
    ///   progress point or on parallel worker completion signals without
    ///   periodic polling.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by built processors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter<R>(mut self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.reporter = Arc::new(reporter);
        self
    }

    /// Sets the shared progress reporter used by built processors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter_arc(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Disables progress callbacks by using [`NoOpProgressReporter`].
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoOpProgressReporter);
        self
    }

    /// Builds a validated [`ParallelBatchProcessor`].
    ///
    /// # Returns
    ///
    /// A parallel batch processor when the configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchProcessorBuildError`] when the worker count is
    /// zero.
    #[inline]
    pub fn build(self) -> Result<ParallelBatchProcessor<Item>, ParallelBatchProcessorBuildError> {
        let thread_count = NonZeroUsize::new(self.thread_count)
            .ok_or(ParallelBatchProcessorBuildError::ZeroThreadCount)?;
        Ok(ParallelBatchProcessor {
            consumer: self.consumer,
            thread_count,
            sequential_threshold: self.sequential_threshold,
            report_interval: self.report_interval,
            reporter: self.reporter,
        })
    }
}
