/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use crate::BatchExecutionResult;

/// Result produced by [`crate::BatchExecutor::call`].
///
/// The execution result contains the same failure aggregation as
/// [`crate::BatchExecutor::execute`]. The value list is indexed by the original
/// callable index; successful callables store `Some(value)`, while failed or
/// panicked callables store `None`.
///
/// # Type Parameters
///
/// * `R` - Callable success value type.
/// * `E` - Callable error type.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCallResult<R, E> {
    /// Execution summary and failures for the callable batch.
    execution_result: BatchExecutionResult<E>,
    /// Success values indexed by callable position.
    values: Vec<Option<R>>,
}

impl<R, E> BatchCallResult<R, E> {
    /// Creates a new callable batch result.
    ///
    /// # Parameters
    ///
    /// * `execution_result` - Execution summary and failures.
    /// * `values` - Success values indexed by callable position.
    ///
    /// # Returns
    ///
    /// A callable batch result.
    #[inline]
    pub fn new(execution_result: BatchExecutionResult<E>, values: Vec<Option<R>>) -> Self {
        Self {
            execution_result,
            values,
        }
    }

    /// Returns the execution summary for the callable batch.
    ///
    /// # Returns
    ///
    /// A shared reference to the underlying execution result.
    #[inline]
    pub const fn execution_result(&self) -> &BatchExecutionResult<E> {
        &self.execution_result
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

    /// Consumes this result and returns the execution summary.
    ///
    /// # Returns
    ///
    /// The underlying execution result.
    #[inline]
    pub fn into_execution_result(self) -> BatchExecutionResult<E> {
        self.execution_result
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
    /// A tuple containing the execution result and indexed success values.
    #[inline]
    pub fn into_parts(self) -> (BatchExecutionResult<E>, Vec<Option<R>>) {
        (self.execution_result, self.values)
    }
}
