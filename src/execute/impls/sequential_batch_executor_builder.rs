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

use super::SequentialBatchExecutor;

/// Builder for [`SequentialBatchExecutor`].
///
/// Use the builder when the default progress interval or reporter should be
/// customized.
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::SequentialBatchExecutor;
///
/// let executor = SequentialBatchExecutor::builder()
///     .report_interval(Duration::ZERO)
///     .build();
///
/// assert_eq!(executor.report_interval(), Duration::ZERO);
/// ```
pub struct SequentialBatchExecutorBuilder {
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl SequentialBatchExecutorBuilder {
    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every sequential
    ///   between-task progress point.
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
    /// * `reporter` - Progress reporter used for later executions.
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
    /// * `reporter` - Shared progress reporter used for later executions.
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

    /// Builds a [`SequentialBatchExecutor`].
    ///
    /// # Returns
    ///
    /// A sequential batch executor with this builder's configuration.
    #[inline]
    pub fn build(self) -> SequentialBatchExecutor {
        SequentialBatchExecutor {
            report_interval: self.report_interval,
            reporter: self.reporter,
        }
    }
}

impl Default for SequentialBatchExecutorBuilder {
    /// Creates a builder with default sequential batch settings.
    ///
    /// # Returns
    ///
    /// A builder using five-second progress intervals and no-op reporting.
    fn default() -> Self {
        Self {
            report_interval: SequentialBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }
}
