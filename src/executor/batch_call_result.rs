/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use crate::BatchOutcome;

/// Result produced by [`crate::BatchExecutor::call`].
///
/// The execution outcome contains the same failure aggregation as
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
    /// A callable batch result.
    #[inline]
    pub fn new(outcome: BatchOutcome<E>, values: Vec<Option<R>>) -> Self {
        Self { outcome, values }
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
