/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::{
    sync::Arc,
    time::Duration,
};

use crate::progress::ProgressReporter;

use super::parallel_batch_executor::{
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    default_reporter,
    default_thread_name_prefix,
};

/// Builder for [`ParallelBatchExecutor`].
///
/// # Author
///
/// Haixing Hu
pub struct ParallelBatchExecutorBuilder {
    /// Number of Rayon worker threads to create.
    parallelism: usize,
    /// Maximum batch size that still uses sequential execution.
    parallel_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
    /// Prefix used when naming Rayon worker threads.
    thread_name_prefix: String,
    /// Optional worker stack size in bytes.
    stack_size: Option<usize>,
}

impl ParallelBatchExecutorBuilder {
    /// Sets the Rayon worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `parallelism` - Number of Rayon worker threads.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Sets the sequential fallback threshold.
    ///
    /// # Parameters
    ///
    /// * `parallel_threshold` - Maximum batch size that still runs sequentially.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn parallel_threshold(mut self, parallel_threshold: usize) -> Self {
        self.parallel_threshold = parallel_threshold;
        self
    }

    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum interval between progress callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by the executor.
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

    /// Sets the progress reporter used by the executor.
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

    /// Resets the progress reporter to the no-op implementation.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = default_reporter();
        self
    }

    /// Sets the Rayon worker-thread name prefix.
    ///
    /// # Parameters
    ///
    /// * `thread_name_prefix` - Prefix appended with the worker index.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn thread_name_prefix(mut self, thread_name_prefix: &str) -> Self {
        self.thread_name_prefix = thread_name_prefix.to_owned();
        self
    }

    /// Sets the Rayon worker-thread stack size.
    ///
    /// # Parameters
    ///
    /// * `stack_size` - Stack size in bytes for each worker thread.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = Some(stack_size);
        self
    }

    /// Builds the configured parallel batch executor.
    ///
    /// # Returns
    ///
    /// A configured parallel batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError`] when the supplied
    /// configuration is invalid or Rayon rejects it.
    pub fn build(self) -> Result<ParallelBatchExecutor, ParallelBatchExecutorBuildError> {
        ParallelBatchExecutor::from_parts(
            self.parallelism,
            self.parallel_threshold,
            self.report_interval,
            self.reporter,
            self.thread_name_prefix,
            self.stack_size,
        )
    }
}

impl Default for ParallelBatchExecutorBuilder {
    /// Creates a builder with default parallel batch settings.
    ///
    /// # Returns
    ///
    /// A builder configured with default Rayon and progress-report settings.
    fn default() -> Self {
        Self {
            parallelism: ParallelBatchExecutor::default_parallelism(),
            parallel_threshold: ParallelBatchExecutor::DEFAULT_PARALLEL_THRESHOLD,
            report_interval: ParallelBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: default_reporter(),
            thread_name_prefix: default_thread_name_prefix(),
            stack_size: None,
        }
    }
}
