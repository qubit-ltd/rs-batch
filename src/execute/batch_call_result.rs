// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::BatchCallResultBuildError;
use crate::BatchOutcome;

/// Result produced by [`crate::BatchExecutor::call`].
///
/// The execution outcome contains the same failure aggregation as
/// [`crate::BatchExecutor::execute`]. The value list is indexed by the original
/// callable index; successful callables store `Some(value)`, while failed or
/// panicked callables store `None`.
///
/// ```rust
/// use qubit_batch::{
///     BatchExecutor,
///     SequentialBatchExecutor,
/// };
///
/// fn count_users() -> Result<usize, &'static str> {
///     Ok(3)
/// }
///
/// fn count_orders() -> Result<usize, &'static str> {
///     Ok(5)
/// }
///
/// let result = SequentialBatchExecutor::new()
///     .call([count_users, count_orders])
///     .expect("array length should be exact");
///
/// assert!(result.outcome().is_success());
/// assert_eq!(result.values(), &[Some(3), Some(5)]);
/// ```
///
/// # Type Parameters
///
/// * `R` - Callable success value type.
/// * `E` - Callable error type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "batch call results contain execution failures and returned values"]
pub struct BatchCallResult<R, E> {
    /// Execution outcome and failures for the callable batch.
    outcome: BatchOutcome<E>,
    /// Success values indexed by callable position.
    values: Vec<Option<R>>,
}

impl<R, E> BatchCallResult<R, E> {
    /// Creates a new callable batch result.
    ///
    /// # Parameters
    ///
    /// * `outcome` - Execution outcome and failures.
    /// * `values` - Success values indexed by callable position.
    ///
    /// # Returns
    ///
    /// A callable batch result when `values` has one entry per declared task,
    /// every failed or panicked callable has `None`, and the number of present
    /// values equals the successful task count.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallResultBuildError`] when the value vector length,
    /// present value count, or failure-index mapping disagrees with `outcome`.
    #[inline]
    pub fn try_new(
        outcome: BatchOutcome<E>,
        values: Vec<Option<R>>,
    ) -> Result<Self, BatchCallResultBuildError> {
        let task_count = outcome.task_count();
        let value_count = values.len();
        if value_count != task_count {
            return Err(BatchCallResultBuildError::ValueCountMismatch {
                task_count,
                value_count,
            });
        }
        for failure in outcome.failures() {
            if values[failure.index()].is_some() {
                return Err(BatchCallResultBuildError::FailureValuePresent {
                    index: failure.index(),
                });
            }
        }
        let succeeded_count = outcome.succeeded_count();
        let value_count = values.iter().filter(|value| value.is_some()).count();
        if value_count != succeeded_count {
            return Err(
                BatchCallResultBuildError::SucceededValueCountMismatch {
                    succeeded_count,
                    value_count,
                },
            );
        }
        Ok(Self { outcome, values })
    }

    /// Returns the execution outcome for the callable batch.
    ///
    /// # Returns
    ///
    /// A shared reference to the underlying execution outcome.
    #[inline]
    pub const fn outcome(&self) -> &BatchOutcome<E> {
        &self.outcome
    }

    /// Returns success values indexed by callable position.
    ///
    /// # Returns
    ///
    /// A shared slice of optional success values.
    #[inline]
    pub fn values(&self) -> &[Option<R>] {
        self.values.as_slice()
    }

    /// Consumes this result and returns the execution outcome.
    ///
    /// # Returns
    ///
    /// The underlying execution outcome.
    #[inline]
    pub fn into_outcome(self) -> BatchOutcome<E> {
        self.outcome
    }

    /// Consumes this result and returns success values.
    ///
    /// # Returns
    ///
    /// Success values indexed by callable position.
    #[inline]
    pub fn into_values(self) -> Vec<Option<R>> {
        self.values
    }

    /// Consumes this result and returns both stored parts.
    ///
    /// # Returns
    ///
    /// A tuple containing the execution outcome and indexed success values.
    #[inline]
    pub fn into_parts(self) -> (BatchOutcome<E>, Vec<Option<R>>) {
        (self.outcome, self.values)
    }
}
