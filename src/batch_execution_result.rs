/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::{
    fmt,
    time::Duration,
};

use crate::{
    BatchTaskError,
    BatchTaskFailure,
};

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
    /// Detailed failure records sorted by task index.
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
    /// A fully populated batch execution result with failures sorted by task
    /// index.
    ///
    /// # Panics
    ///
    /// Panics when the supplied counters are inconsistent, when the detailed
    /// failure list does not match `failed_count` and `panicked_count`, or when
    /// any failure index is outside `0..task_count`.
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
        validate_result_invariants(
            task_count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            &failures,
        );
        let mut failures = failures;
        failures.sort_by_key(|failure| failure.index());
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

impl<E> fmt::Display for BatchExecutionResult<E> {
    /// Formats a concise summary of this batch execution result.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the summary text.
    ///
    /// # Returns
    ///
    /// The formatting result produced by `write!`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatchExecutionResult {{ task_count: {}, completed_count: {}, succeeded_count: {}, failed_count: {}, panicked_count: {}, elapsed: {:?} }}",
            self.task_count(),
            self.completed_count(),
            self.succeeded_count(),
            self.failed_count(),
            self.panicked_count(),
            self.elapsed(),
        )
    }
}

/// Validates all counters and failure details for a batch result.
///
/// # Parameters
///
/// * `task_count` - Declared batch task count.
/// * `completed_count` - Number of completed tasks.
/// * `succeeded_count` - Number of successful tasks.
/// * `failed_count` - Number of failed tasks.
/// * `panicked_count` - Number of panicked tasks.
/// * `failures` - Detailed failure records to validate.
///
/// # Panics
///
/// Panics when the aggregate counters, detailed failure counts, or failure
/// indexes are inconsistent.
fn validate_result_invariants<E>(
    task_count: usize,
    completed_count: usize,
    succeeded_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &[BatchTaskFailure<E>],
) {
    let failure_count = failed_count
        .checked_add(panicked_count)
        .expect("failed and panicked task counts must not overflow");
    let terminal_count = succeeded_count
        .checked_add(failure_count)
        .expect("terminal task counts must not overflow");

    assert!(
        completed_count <= task_count,
        "completed task count must not exceed declared task count"
    );
    assert_eq!(
        terminal_count, completed_count,
        "completed task count must equal succeeded + failed + panicked counts"
    );
    assert_eq!(
        failures.len(),
        failure_count,
        "failure detail count must equal failed + panicked counts"
    );
    validate_failure_details(task_count, failed_count, panicked_count, failures);
}

/// Validates detailed failure records against their aggregate counters.
///
/// # Parameters
///
/// * `task_count` - Declared batch task count.
/// * `failed_count` - Expected number of task errors.
/// * `panicked_count` - Expected number of task panics.
/// * `failures` - Detailed failure records to inspect.
///
/// # Panics
///
/// Panics when a failure index is outside the batch range or when the observed
/// failure variants do not match the aggregate counters.
fn validate_failure_details<E>(
    task_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &[BatchTaskFailure<E>],
) {
    let mut observed_failed_count = 0usize;
    let mut observed_panicked_count = 0usize;
    for failure in failures {
        assert!(
            failure.index() < task_count,
            "failure index must be less than declared task count"
        );
        match failure.error() {
            BatchTaskError::Failed(_) => observed_failed_count += 1,
            BatchTaskError::Panicked => observed_panicked_count += 1,
        }
    }
    assert_eq!(
        observed_failed_count, failed_count,
        "failed detail count must match failed_count"
    );
    assert_eq!(
        observed_panicked_count, panicked_count,
        "panicked detail count must match panicked_count"
    );
}
