/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::time::Duration;

use crate::BatchTaskFailure;

/// Structured result produced by one batch execution.
///
/// The result always describes the work that actually ran, even when the outer
/// batch API returns a [`crate::BatchExecutionError`] due to task-count
/// mismatches.
///
/// # Type Parameters
///
/// * `E` - The task-specific error type.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug)]
pub struct BatchExecutionResult<E> {
    /// Declared task count for this batch.
    task_count: usize,
    /// Number of tasks that reached a terminal outcome.
    completed_count: usize,
    /// Number of tasks that completed successfully.
    succeeded_count: usize,
    /// Number of tasks that returned their own error.
    failed_count: usize,
    /// Number of tasks that panicked.
    panicked_count: usize,
    /// Total elapsed wall-clock time for the batch.
    elapsed: Duration,
    /// Detailed failure records in task-index order of discovery.
    failures: Vec<BatchTaskFailure<E>>,
}

impl<E> BatchExecutionResult<E> {
    /// Creates a new batch execution result.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared task count for the batch.
    /// * `completed_count` - Number of tasks that finished.
    /// * `succeeded_count` - Number of tasks that succeeded.
    /// * `failed_count` - Number of tasks that returned their own error.
    /// * `panicked_count` - Number of tasks that panicked.
    /// * `elapsed` - Total elapsed wall-clock time.
    /// * `failures` - Detailed task failure records.
    ///
    /// # Returns
    ///
    /// A fully populated batch execution result.
    #[inline]
    pub fn new(
        task_count: usize,
        completed_count: usize,
        succeeded_count: usize,
        failed_count: usize,
        panicked_count: usize,
        elapsed: Duration,
        failures: Vec<BatchTaskFailure<E>>,
    ) -> Self {
        Self {
            task_count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            elapsed,
            failures,
        }
    }

    /// Returns the declared task count for this batch.
    ///
    /// # Returns
    ///
    /// The expected number of tasks supplied by the caller.
    #[inline]
    pub const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns how many tasks reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The number of completed tasks.
    #[inline]
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Returns how many tasks completed successfully.
    ///
    /// # Returns
    ///
    /// The number of successful tasks.
    #[inline]
    pub const fn succeeded_count(&self) -> usize {
        self.succeeded_count
    }

    /// Returns how many tasks returned their own error.
    ///
    /// # Returns
    ///
    /// The number of failed tasks.
    #[inline]
    pub const fn failed_count(&self) -> usize {
        self.failed_count
    }

    /// Returns how many tasks panicked.
    ///
    /// # Returns
    ///
    /// The number of panicked tasks.
    #[inline]
    pub const fn panicked_count(&self) -> usize {
        self.panicked_count
    }

    /// Returns the total number of task failures.
    ///
    /// # Returns
    ///
    /// `failed_count + panicked_count`.
    #[inline]
    pub const fn failure_count(&self) -> usize {
        self.failed_count + self.panicked_count
    }

    /// Returns the total elapsed wall-clock time.
    ///
    /// # Returns
    ///
    /// The elapsed duration for this batch execution.
    #[inline]
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns the detailed failure records collected during execution.
    ///
    /// # Returns
    ///
    /// A shared slice of task failure records.
    #[inline]
    pub fn failures(&self) -> &[BatchTaskFailure<E>] {
        self.failures.as_slice()
    }

    /// Returns whether every task completed successfully.
    ///
    /// # Returns
    ///
    /// `true` if the batch has no failures and every declared task completed.
    #[inline]
    pub const fn is_success(&self) -> bool {
        self.completed_count == self.task_count
            && self.failed_count == 0
            && self.panicked_count == 0
    }

    /// Consumes this result and returns its failure list.
    ///
    /// # Returns
    ///
    /// The detailed failure records collected during execution.
    #[inline]
    pub fn into_failures(self) -> Vec<BatchTaskFailure<E>> {
        self.failures
    }
}
