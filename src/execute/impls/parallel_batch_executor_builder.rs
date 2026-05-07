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
    sync::Arc,
    time::Duration,
};

use qubit_progress::reporter::{
    NoOpProgressReporter,
    ProgressReporter,
};

use super::{
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
};

/// Builder for [`ParallelBatchExecutor`].
///
/// Use the builder when the default worker count, sequential fallback
/// threshold, progress interval, or reporter should be customized.
///
/// ```rust
/// use qubit_batch::ParallelBatchExecutor;
///
/// let executor = ParallelBatchExecutor::builder()
///     .thread_count(2)
///     .sequential_threshold(0)
///     .build()
///     .expect("parallel executor configuration should be valid");
///
/// assert_eq!(executor.thread_count(), 2);
/// assert_eq!(executor.sequential_threshold(), 0);
/// ```
pub struct ParallelBatchExecutorBuilder {
    /// Number of worker threads used for parallel executions.
    thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl ParallelBatchExecutorBuilder {
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
    /// * `sequential_threshold` - Maximum batch size that still runs
    ///   sequentially.
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
    /// * `report_interval` - Minimum interval between due-based running
    ///   progress events. Use [`Duration::ZERO`] to report at every
    ///   implementation-defined running progress point.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by built executors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Reporter receiving batch lifecycle callbacks.
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

    /// Sets the shared progress reporter used by built executors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared reporter receiving batch lifecycle callbacks.
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

    /// Builds a validated [`ParallelBatchExecutor`].
    ///
    /// # Returns
    ///
    /// A parallel batch executor when the configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError`] when the worker count is
    /// zero.
    pub fn build(self) -> Result<ParallelBatchExecutor, ParallelBatchExecutorBuildError> {
        if self.thread_count == 0 {
            return Err(ParallelBatchExecutorBuildError::ZeroThreadCount);
        }
        Ok(ParallelBatchExecutor {
            thread_count: self.thread_count,
            sequential_threshold: self.sequential_threshold,
            report_interval: self.report_interval,
            reporter: self.reporter,
        })
    }
}

impl Default for ParallelBatchExecutorBuilder {
    /// Creates a builder with default parallel batch settings.
    ///
    /// # Returns
    ///
    /// A builder using available parallelism, five-second progress intervals,
    /// sequential fallback for batches at or below [`ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD`],
    /// and no-op reporting.
    fn default() -> Self {
        Self {
            thread_count: ParallelBatchExecutor::default_thread_count(),
            sequential_threshold: ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: ParallelBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }
}
