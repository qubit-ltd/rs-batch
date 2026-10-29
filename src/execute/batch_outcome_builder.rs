// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use crate::BatchOutcomeBuildError;
use crate::BatchTaskFailure;
use crate::BatchTermination;
use crate::execute::internal::validate_outcome_invariants;

/// Builder carrying validated parts for a [`crate::BatchOutcome`].
///
/// The builder checks aggregate counters, failure detail count, duplicate
/// indexes, and failed-versus-panicked detail counts before creating an
/// outcome.
///
/// # Type Parameters
///
/// * `E` - Task-specific error type stored in failure records.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::BatchOutcomeBuilder;
/// use qubit_batch::BatchTaskError;
/// use qubit_batch::BatchTaskFailure;
///
/// let outcome = BatchOutcomeBuilder::builder(2)
///     .completed_count(2)
///     .succeeded_count(1)
///     .failed_count(1)
///     .failures(vec![BatchTaskFailure::new(
///         1,
///         BatchTaskError::Failed("invalid row"),
///     )])
///     .build()
///     .expect("outcome counters should match failure details");
///
/// assert_eq!(outcome.failed_count(), 1);
/// assert_eq!(outcome.failures()[0].index(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "configure and build the value before discarding this builder"]
pub struct BatchOutcomeBuilder<E> {
    /// Declared task count for the batch.
    pub(crate) task_count: usize,
    /// Number of tasks that reached a terminal outcome.
    pub(crate) completed_count: usize,
    /// Number of tasks that completed successfully.
    pub(crate) succeeded_count: usize,
    /// Number of tasks that returned their own error.
    pub(crate) failed_count: usize,
    /// Number of tasks that panicked.
    pub(crate) panicked_count: usize,
    /// How execution stopped consuming the task source.
    pub(crate) termination: BatchTermination,
    /// Total monotonic elapsed duration for the batch.
    pub(crate) elapsed: Duration,
    /// Failure records in caller order until validation sorts them by index.
    pub(crate) failures: Vec<BatchTaskFailure<E>>,
}

impl<E> BatchOutcomeBuilder<E> {
    /// Starts building a batch outcome.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared task count for the batch.
    ///
    /// # Returns
    ///
    /// A builder initialized with zero counters, zero elapsed time, and no
    /// failures.
    #[inline]
    pub fn builder(task_count: usize) -> Self {
        Self {
            task_count,
            completed_count: 0,
            succeeded_count: 0,
            failed_count: 0,
            panicked_count: 0,
            termination: BatchTermination::Finished,
            elapsed: Duration::ZERO,
            failures: Vec::new(),
        }
    }

    /// Sets the number of tasks that finished.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of tasks that reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn completed_count(mut self, completed_count: usize) -> Self {
        self.completed_count = completed_count;
        self
    }

    /// Sets the number of successful tasks.
    ///
    /// # Parameters
    ///
    /// * `succeeded_count` - Number of tasks that completed successfully.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn succeeded_count(mut self, succeeded_count: usize) -> Self {
        self.succeeded_count = succeeded_count;
        self
    }

    /// Sets the number of tasks that returned their own error.
    ///
    /// # Parameters
    ///
    /// * `failed_count` - Number of tasks that failed with task errors.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn failed_count(mut self, failed_count: usize) -> Self {
        self.failed_count = failed_count;
        self
    }

    /// Sets the number of tasks that panicked.
    ///
    /// # Parameters
    ///
    /// * `panicked_count` - Number of tasks that panicked.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn panicked_count(mut self, panicked_count: usize) -> Self {
        self.panicked_count = panicked_count;
        self
    }

    /// Sets how execution stopped consuming the task source.
    ///
    /// # Parameters
    ///
    /// * `termination` - Final source-consumption state for the batch.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn termination(mut self, termination: BatchTermination) -> Self {
        self.termination = termination;
        self
    }

    /// Sets the total monotonic elapsed duration.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn elapsed(mut self, elapsed: Duration) -> Self {
        self.elapsed = elapsed;
        self
    }

    /// Sets the detailed failure records.
    ///
    /// # Parameters
    ///
    /// * `failures` - Detailed task failure records.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub fn failures(mut self, failures: Vec<BatchTaskFailure<E>>) -> Self {
        self.failures = failures;
        self
    }

    /// Validates this builder and sorts failure records in place by task index.
    ///
    /// Validation uses no auxiliary index collection. With F failures, sorting
    /// takes O(F log F) worst-case time and does not allocate another buffer.
    /// Errors are checked in this order: aggregate counters, the first
    /// out-of-range index in input order, the smallest duplicate index, then
    /// failed-versus-panicked detail counts.
    ///
    /// # Returns
    ///
    /// `Ok(builder)` when the counters and failure details are consistent.
    ///
    /// # Errors
    ///
    /// Returns [`BatchOutcomeBuildError`] when the counters or failure details
    /// are inconsistent.
    pub fn validate(mut self) -> Result<Self, BatchOutcomeBuildError> {
        validate_outcome_invariants(
            self.task_count,
            self.completed_count,
            self.succeeded_count,
            self.failed_count,
            self.panicked_count,
            &mut self.failures,
        )?;
        Ok(self)
    }

    /// Validates this builder and creates a batch outcome.
    ///
    /// # Returns
    ///
    /// `Ok(outcome)` when the counters and failure details are consistent.
    ///
    /// # Errors
    ///
    /// Returns [`BatchOutcomeBuildError`] when the counters or failure details
    /// are inconsistent.
    #[inline]
    pub fn build(self) -> Result<crate::BatchOutcome<E>, BatchOutcomeBuildError> {
        self.validate().map(crate::BatchOutcome::new)
    }
}
