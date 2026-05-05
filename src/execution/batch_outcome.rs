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
    fmt,
    time::Duration,
};

use crate::{
    BatchOutcomeBuildError,
    BatchTaskError,
    BatchTaskFailure,
};

/// Final or partial outcome produced by one batch execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOutcome<E> {
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
    /// Total monotonic elapsed duration for the batch.
    elapsed: Duration,
    /// Detailed failure records sorted by task index.
    failures: Vec<BatchTaskFailure<E>>,
}

impl<E> BatchOutcome<E> {
    /// Tries to create a new batch outcome.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared task count for the batch.
    /// * `completed_count` - Number of tasks that finished.
    /// * `succeeded_count` - Number of tasks that succeeded.
    /// * `failed_count` - Number of tasks that returned their own error.
    /// * `panicked_count` - Number of tasks that panicked.
    /// * `elapsed` - Total monotonic elapsed duration.
    /// * `failures` - Detailed task failure records.
    ///
    /// # Returns
    ///
    /// `Ok(outcome)` with failures sorted by task index when the counters and
    /// failure details are consistent.
    ///
    /// # Errors
    ///
    /// Returns [`BatchOutcomeBuildError`] when the supplied counters are
    /// inconsistent.
    pub fn try_new(
        task_count: usize,
        completed_count: usize,
        succeeded_count: usize,
        failed_count: usize,
        panicked_count: usize,
        elapsed: Duration,
        failures: Vec<BatchTaskFailure<E>>,
    ) -> Result<Self, BatchOutcomeBuildError> {
        validate_outcome_invariants(
            task_count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            &failures,
        )?;
        let mut failures = failures;
        failures.sort_by_key(|failure| failure.index());
        Ok(Self {
            task_count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            elapsed,
            failures,
        })
    }

    /// Creates a new batch outcome for executor-internal use.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared task count for the batch.
    /// * `completed_count` - Number of tasks that finished.
    /// * `succeeded_count` - Number of tasks that succeeded.
    /// * `failed_count` - Number of tasks that returned their own error.
    /// * `panicked_count` - Number of tasks that panicked.
    /// * `elapsed` - Total monotonic elapsed duration.
    /// * `failures` - Detailed task failure records.
    ///
    /// # Returns
    ///
    /// A fully populated batch outcome with failures sorted by task index.
    ///
    /// # Panics
    ///
    /// Panics when the executor supplies inconsistent counters.
    #[track_caller]
    pub(crate) fn from_validated_parts(
        task_count: usize,
        completed_count: usize,
        succeeded_count: usize,
        failed_count: usize,
        panicked_count: usize,
        elapsed: Duration,
        failures: Vec<BatchTaskFailure<E>>,
    ) -> Self {
        Self::try_new(
            task_count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            elapsed,
            failures,
        )
        .expect("batch outcome invariants must hold")
    }

    /// Returns the declared task count for this batch.
    ///
    /// # Returns
    ///
    /// The expected number of tasks supplied by the caller.
    pub const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns how many tasks reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The number of completed tasks.
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Returns how many tasks completed successfully.
    ///
    /// # Returns
    ///
    /// The number of successful tasks.
    pub const fn succeeded_count(&self) -> usize {
        self.succeeded_count
    }

    /// Returns how many tasks returned their own error.
    ///
    /// # Returns
    ///
    /// The number of failed tasks.
    pub const fn failed_count(&self) -> usize {
        self.failed_count
    }

    /// Returns how many tasks panicked.
    ///
    /// # Returns
    ///
    /// The number of panicked tasks.
    pub const fn panicked_count(&self) -> usize {
        self.panicked_count
    }

    /// Returns the total number of task failures.
    ///
    /// # Returns
    ///
    /// Failed plus panicked task count.
    pub const fn failure_count(&self) -> usize {
        self.failed_count + self.panicked_count
    }

    /// Returns the total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The elapsed duration for this batch execution.
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns the detailed failure records collected during execution.
    ///
    /// # Returns
    ///
    /// A shared slice of task failure records.
    pub fn failures(&self) -> &[BatchTaskFailure<E>] {
        self.failures.as_slice()
    }

    /// Returns whether every task completed successfully.
    ///
    /// # Returns
    ///
    /// `true` if the batch has no failures and every declared task completed.
    pub const fn is_success(&self) -> bool {
        self.completed_count == self.task_count
            && self.failed_count == 0
            && self.panicked_count == 0
    }

    /// Consumes this outcome and returns its failure list.
    ///
    /// # Returns
    ///
    /// The detailed failure records collected during execution.
    pub fn into_failures(self) -> Vec<BatchTaskFailure<E>> {
        self.failures
    }
}

impl<E> fmt::Display for BatchOutcome<E> {
    /// Formats a concise summary of this batch outcome.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Formatter receiving the summary text.
    ///
    /// # Returns
    ///
    /// The formatting result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "BatchOutcome {{ task_count: {}, completed_count: {}, succeeded_count: {}, failed_count: {}, panicked_count: {}, elapsed: {:?} }}",
            self.task_count(),
            self.completed_count(),
            self.succeeded_count(),
            self.failed_count(),
            self.panicked_count(),
            self.elapsed(),
        )
    }
}

/// Validates all counters and failure details for a batch outcome.
fn validate_outcome_invariants<E>(
    task_count: usize,
    completed_count: usize,
    succeeded_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &[BatchTaskFailure<E>],
) -> Result<(), BatchOutcomeBuildError> {
    let failure_count = failed_count.checked_add(panicked_count).ok_or(
        BatchOutcomeBuildError::FailureCountOverflow {
            failed_count,
            panicked_count,
        },
    )?;
    let terminal_count = succeeded_count.checked_add(failure_count).ok_or(
        BatchOutcomeBuildError::TerminalCountOverflow {
            succeeded_count,
            failure_count,
        },
    )?;

    if completed_count > task_count {
        return Err(BatchOutcomeBuildError::CompletedCountExceeded {
            task_count,
            completed_count,
        });
    }
    if terminal_count != completed_count {
        return Err(BatchOutcomeBuildError::TerminalCountMismatch {
            completed_count,
            terminal_count,
            succeeded_count,
            failed_count,
            panicked_count,
        });
    }
    if failures.len() != failure_count {
        return Err(BatchOutcomeBuildError::FailureDetailCountMismatch {
            expected: failure_count,
            actual: failures.len(),
        });
    }
    validate_failure_details(task_count, failed_count, panicked_count, failures)
}

/// Validates detailed failure records against aggregate counters.
fn validate_failure_details<E>(
    task_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &[BatchTaskFailure<E>],
) -> Result<(), BatchOutcomeBuildError> {
    let mut observed_failed_count = 0usize;
    let mut observed_panicked_count = 0usize;
    for failure in failures {
        if failure.index() >= task_count {
            return Err(BatchOutcomeBuildError::FailureIndexOutOfRange {
                index: failure.index(),
                task_count,
            });
        }
        match failure.error() {
            BatchTaskError::Failed(_) => observed_failed_count += 1,
            BatchTaskError::Panicked { .. } => observed_panicked_count += 1,
        }
    }
    if observed_failed_count != failed_count || observed_panicked_count != panicked_count {
        return Err(BatchOutcomeBuildError::FailureVariantCountMismatch {
            expected_failed: failed_count,
            actual_failed: observed_failed_count,
            expected_panicked: panicked_count,
            actual_panicked: observed_panicked_count,
        });
    }
    Ok(())
}
