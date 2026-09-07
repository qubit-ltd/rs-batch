// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;

use thiserror::Error;

use crate::BatchOutcome;
use crate::ProgressFailure;

/// Batch-level error returned when progress, source-count validation, or
/// parallel scheduling fails.
///
/// Task failures are reported through [`BatchOutcome`], not through
/// this enum. This error reports progress-reporter failures and declared
/// task-count mismatches, and incomplete parallel schedules.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::{
///     BatchExecutionError,
///     BatchExecutor,
///     SequentialBatchExecutor,
/// };
///
/// let error = SequentialBatchExecutor::new()
///     .for_each_with_count([10, 20], 3, |_value| Ok::<(), &'static str>(()))
///     .expect_err("iterator should yield fewer items than declared");
///
/// assert!(error.is_count_shortfall());
/// assert_eq!(error.outcome().completed_count(), 2);
/// match error {
///     BatchExecutionError::CountShortfall { expected, actual, .. } => {
///         assert_eq!(expected, 3);
///         assert_eq!(actual, 2);
///     }
///     BatchExecutionError::ProgressReport { .. } => unreachable!(),
///     BatchExecutionError::CountExceeded { .. } => unreachable!(),
///     _ => unreachable!(),
/// }
/// ```
///
/// # Type Parameters
///
/// * `E` - The task-specific error type stored inside the attached outcome.
/// * `S` - The runtime scheduler error type. It defaults to
///   [`std::convert::Infallible`] for built-in executors.
#[non_exhaustive]
#[must_use = "errors describe a rejected operation"]
#[derive(Debug, Error)]
pub enum BatchExecutionError<E, S = Infallible> {
    /// Reporting batch progress failed.
    #[error("batch progress reporting failed")]
    ProgressReport {
        /// Reporter error returned by the configured progress sink.
        #[source]
        source: Box<ProgressFailure>,
        /// Outcome accumulated before reporting failed.
        outcome: BatchOutcome<E>,
    },

    /// The runtime scheduler rejected or could not submit work.
    #[error("batch scheduler failed: {source}")]
    ScheduleFailed {
        /// Scheduler error returned by the runtime integration.
        #[source]
        source: S,
        /// Outcome accumulated before scheduling failed.
        outcome: BatchOutcome<E>,
        /// Additional progress-reporting error observed while reporting this
        /// primary scheduler error.
        report_error: Option<Box<ProgressFailure>>,
    },

    /// The task source ended before the declared task count was reached.
    #[error("batch task count shortfall: expected {expected}, actual {actual}")]
    CountShortfall {
        /// Declared task count.
        expected: usize,
        /// Actual number of tasks observed from the source.
        actual: usize,
        /// Outcome accumulated from the tasks that did run.
        outcome: BatchOutcome<E>,
        /// Additional progress-reporting error observed while reporting this
        /// primary count error.
        report_error: Option<Box<ProgressFailure>>,
    },

    /// The task source yielded more tasks than the declared task count.
    #[error("batch task count exceeded: expected {expected}, observed at least {observed_at_least}")]
    CountExceeded {
        /// Declared task count.
        expected: usize,
        /// Lower bound of observed tasks. This is typically `expected + 1`
        /// because the executor stops reading once it confirms the overflow.
        observed_at_least: usize,
        /// Outcome accumulated from the tasks that did run.
        outcome: BatchOutcome<E>,
        /// Additional progress-reporting error observed while reporting this
        /// primary count error.
        report_error: Option<Box<ProgressFailure>>,
    },

    /// The scheduler accepted tasks but did not execute all of them.
    #[error("parallel batch schedule incomplete: expected {expected}, accepted {accepted}, completed {completed}")]
    IncompleteSchedule {
        /// Declared task count.
        expected: usize,
        /// Number of tasks accepted by the scheduler.
        accepted: usize,
        /// Number of source tasks observed before scheduling stopped.
        observed: usize,
        /// Number of accepted tasks that reached a terminal outcome.
        completed: usize,
        /// Outcome accumulated before the incomplete schedule was reported.
        outcome: BatchOutcome<E>,
        /// Additional progress-reporting error observed while reporting this
        /// primary scheduling error.
        report_error: Option<Box<ProgressFailure>>,
    },
}

impl<E, S> BatchExecutionError<E, S>
where
    S: std::error::Error + Send + Sync + 'static,
{
    /// Returns the batch outcome attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the attached batch outcome.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn outcome(&self) -> &BatchOutcome<E> {
        match self {
            Self::ProgressReport { outcome, .. }
            | Self::ScheduleFailed { outcome, .. }
            | Self::CountShortfall { outcome, .. }
            | Self::CountExceeded { outcome, .. }
            | Self::IncompleteSchedule { outcome, .. } => outcome,
        }
    }

    /// Returns whether this error represents a task-count shortfall.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::CountShortfall`].
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn is_count_shortfall(&self) -> bool {
        matches!(self, Self::CountShortfall { .. })
    }

    /// Returns whether this error represents a scheduler failure.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::ScheduleFailed`].
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn is_schedule_failed(&self) -> bool {
        matches!(self, Self::ScheduleFailed { .. })
    }

    /// Returns the scheduler error, when scheduling failed.
    ///
    /// # Returns
    ///
    /// `Some(error)` for [`Self::ScheduleFailed`], or `None` for other errors.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn scheduler_error(&self) -> Option<&S> {
        match self {
            Self::ScheduleFailed { source, .. } => Some(source),
            _ => None,
        }
    }

    /// Returns whether this error represents an oversized task source.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::CountExceeded`].
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn is_count_exceeded(&self) -> bool {
        matches!(self, Self::CountExceeded { .. })
    }

    /// Returns whether this error represents an incomplete parallel schedule.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::IncompleteSchedule`].
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn is_incomplete_schedule(&self) -> bool {
        matches!(self, Self::IncompleteSchedule { .. })
    }

    /// Returns the progress-reporting error associated with this error.
    ///
    /// # Returns
    ///
    /// The reporter error for [`Self::ProgressReport`], an additional reporter
    /// error attached to a primary count/scheduler error, or `None` when
    /// reporting did not fail.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn progress_report_error(&self) -> Option<&ProgressFailure> {
        match self {
            Self::ProgressReport { source, .. } => Some(source.as_ref()),
            Self::ScheduleFailed { report_error, .. }
            | Self::CountShortfall { report_error, .. }
            | Self::CountExceeded { report_error, .. }
            | Self::IncompleteSchedule { report_error, .. } => report_error.as_deref(),
        }
    }

    /// Consumes this error and returns the attached batch outcome.
    ///
    /// # Returns
    ///
    /// The batch outcome accumulated before this error was reported.
    #[inline(always)]
    pub fn into_outcome(self) -> BatchOutcome<E> {
        match self {
            Self::ProgressReport { outcome, .. }
            | Self::ScheduleFailed { outcome, .. }
            | Self::CountShortfall { outcome, .. }
            | Self::CountExceeded { outcome, .. }
            | Self::IncompleteSchedule { outcome, .. } => outcome,
        }
    }

    /// Maps the scheduler error while preserving the attached outcome.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Replacement scheduler error type.
    /// * `F` - Function that converts `S` into `T`.
    ///
    /// # Parameters
    ///
    /// * `map` - Conversion applied only to the scheduler error variant.
    ///
    /// # Returns
    ///
    /// An equivalent error with its scheduler error converted to `T`.
    pub fn map_scheduler_error<T, F>(self, map: F) -> BatchExecutionError<E, T>
    where
        T: std::error::Error + Send + Sync + 'static,
        F: FnOnce(S) -> T,
    {
        match self {
            Self::ProgressReport { source, outcome } => BatchExecutionError::ProgressReport { source, outcome },
            Self::ScheduleFailed {
                source,
                outcome,
                report_error,
            } => BatchExecutionError::ScheduleFailed {
                source: map(source),
                outcome,
                report_error,
            },
            Self::CountShortfall {
                expected,
                actual,
                outcome,
                report_error,
            } => BatchExecutionError::CountShortfall {
                expected,
                actual,
                outcome,
                report_error,
            },
            Self::CountExceeded {
                expected,
                observed_at_least,
                outcome,
                report_error,
            } => BatchExecutionError::CountExceeded {
                expected,
                observed_at_least,
                outcome,
                report_error,
            },
            Self::IncompleteSchedule {
                expected,
                accepted,
                observed,
                completed,
                outcome,
                report_error,
            } => BatchExecutionError::IncompleteSchedule {
                expected,
                accepted,
                observed,
                completed,
                outcome,
                report_error,
            },
        }
    }
}
