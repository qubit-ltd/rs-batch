/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::fmt;

use thiserror::Error;

use crate::BatchExecutionResult;

/// Batch-level error returned when the batch contract is violated.
///
/// Task failures are reported through [`BatchExecutionResult`], not through
/// this enum. This error is reserved for situations such as declared task-count
/// mismatches.
///
/// # Type Parameters
///
/// * `E` - The task-specific error type stored inside the attached result.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug, Error)]
pub enum BatchExecutionError<E> {
    /// The task source ended before the declared task count was reached.
    #[error("batch task count shortfall: expected {expected}, actual {actual}")]
    CountShortfall {
        /// Declared task count.
        expected: usize,
        /// Actual number of tasks observed from the source.
        actual: usize,
        /// Result accumulated from the tasks that did run.
        result: BatchExecutionResult<E>,
    },

    /// The task source yielded more tasks than the declared task count.
    #[error(
        "batch task count exceeded: expected {expected}, observed at least {observed_at_least}"
    )]
    CountExceeded {
        /// Declared task count.
        expected: usize,
        /// Lower bound of observed tasks. This is typically `expected + 1`
        /// because the executor stops reading once it confirms the overflow.
        observed_at_least: usize,
        /// Result accumulated from the tasks that did run.
        result: BatchExecutionResult<E>,
    },
}

impl<E> BatchExecutionError<E> {
    /// Returns the batch result attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the attached batch execution result.
    #[inline]
    pub const fn result(&self) -> &BatchExecutionResult<E> {
        match self {
            Self::CountShortfall { result, .. } | Self::CountExceeded { result, .. } => result,
        }
    }

    /// Consumes this error and returns the attached batch result.
    ///
    /// # Returns
    ///
    /// The batch execution result accumulated before this error was reported.
    #[inline]
    pub fn into_result(self) -> BatchExecutionResult<E> {
        match self {
            Self::CountShortfall { result, .. } | Self::CountExceeded { result, .. } => result,
        }
    }

    /// Returns whether this error represents a task-count shortfall.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::CountShortfall`].
    #[inline]
    pub const fn is_count_shortfall(&self) -> bool {
        matches!(self, Self::CountShortfall { .. })
    }

    /// Returns whether this error represents an oversized task source.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::CountExceeded`].
    #[inline]
    pub const fn is_count_exceeded(&self) -> bool {
        matches!(self, Self::CountExceeded { .. })
    }
}

impl<E> fmt::Display for BatchExecutionResult<E>
where
    E: fmt::Debug,
{
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
