/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use thiserror::Error;

use crate::BatchOutcome;

/// Batch-level error returned when the batch contract is violated.
///
/// Task failures are reported through [`BatchOutcome`], not through
/// this enum. This error is reserved for situations such as declared task-count
/// mismatches.
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
///     BatchExecutionError::CountExceeded { .. } => unreachable!(),
/// }
/// ```
///
/// # Type Parameters
///
/// * `E` - The task-specific error type stored inside the attached outcome.
///
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchExecutionError<E> {
    /// The task source ended before the declared task count was reached.
    #[error("batch task count shortfall: expected {expected}, actual {actual}")]
    CountShortfall {
        /// Declared task count.
        expected: usize,
        /// Actual number of tasks observed from the source.
        actual: usize,
        /// Outcome accumulated from the tasks that did run.
        outcome: BatchOutcome<E>,
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
        /// Outcome accumulated from the tasks that did run.
        outcome: BatchOutcome<E>,
    },
}

impl<E> BatchExecutionError<E> {
    /// Returns the batch outcome attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the attached batch outcome.
    #[inline]
    pub const fn outcome(&self) -> &BatchOutcome<E> {
        match self {
            Self::CountShortfall { outcome, .. } | Self::CountExceeded { outcome, .. } => outcome,
        }
    }

    /// Consumes this error and returns the attached batch outcome.
    ///
    /// # Returns
    ///
    /// The batch outcome accumulated before this error was reported.
    #[inline]
    pub fn into_outcome(self) -> BatchOutcome<E> {
        match self {
            Self::CountShortfall { outcome, .. } | Self::CountExceeded { outcome, .. } => outcome,
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
