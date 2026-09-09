// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::time::Duration;

use qubit_progress::reporter::NoopReporter;
use qubit_progress::reporter::Reporter;

use super::SequentialBatchExecutor;
use crate::TaskFailurePolicy;

/// Builder for [`SequentialBatchExecutor`].
///
/// Use the builder when the default progress interval or reporter should be
/// customized.
///
/// # Examples
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
#[must_use = "configure and build the value before discarding this builder"]
pub struct SequentialBatchExecutorBuilder {
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn Reporter>,
    /// Policy used after task errors or captured task panics.
    task_failure_policy: TaskFailurePolicy,
}

impl SequentialBatchExecutorBuilder {
    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every sequential between-task
    ///   progress point.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the task failure policy used by built executors.
    ///
    /// # Parameters
    ///
    /// * `task_failure_policy` - Policy applied after a task returns an error
    ///   or panics.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn task_failure_policy(mut self, task_failure_policy: TaskFailurePolicy) -> Self {
        self.task_failure_policy = task_failure_policy;
        self
    }

    /// Sets the progress reporter used by built executors.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Concrete reporter type stored behind the shared reporter trait
    ///   object.
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
        R: Reporter + 'static,
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
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn reporter_arc(mut self, reporter: Arc<dyn Reporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Disables progress callbacks by using [`NoopReporter`].
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoopReporter);
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
            task_failure_policy: self.task_failure_policy,
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
            reporter: Arc::new(NoopReporter),
            task_failure_policy: TaskFailurePolicy::default(),
        }
    }
}
