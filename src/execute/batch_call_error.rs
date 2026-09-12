// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::fmt;

use super::BatchCallOutput;
use crate::BatchExecutionError;
use crate::BatchOutcome;

/// Batch-level callable error that preserves successful values collected before
/// execution stopped.
///
/// The nested [`BatchExecutionError`] retains the partial execution outcome.
/// `outputs` contains only callables that completed successfully, sorted by
/// their original zero-based indexes.
///
/// # Type Parameters
///
/// * `R` - Callable success value type.
/// * `E` - Callable error type stored in the nested execution outcome.
/// * `S` - Scheduler error type stored in the nested execution error.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::SequentialBatchExecutor;
/// let error = SequentialBatchExecutor::new()
///     .call_with_count([|| Ok::<_, &'static str>(42)], 2)
///     .expect_err("the declared count exceeds the source length");
/// assert!(error.source().is_count_shortfall());
/// assert_eq!(*error.outputs()[0].value(), 42);
/// ```
#[must_use = "call errors preserve partial callable values"]
pub struct BatchCallError<R, E, S = Infallible>
where
    S: std::error::Error + Send + Sync + 'static,
{
    /// Batch-level failure with counters for this call only.
    source: Box<BatchExecutionError<E, S>>,
    /// Successful outputs from this call, sorted by original input index.
    outputs: Vec<BatchCallOutput<R>>,
}

impl<R, E, S> fmt::Debug for BatchCallError<R, E, S>
where
    E: fmt::Debug,
    S: std::error::Error + Send + Sync + 'static,
{
    /// Formats the nested error and the number of preserved outputs.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// The formatting result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchCallError")
            .field("source", &self.source)
            .field("output_count", &self.outputs.len())
            .finish()
    }
}

impl<R, E, S> BatchCallError<R, E, S>
where
    S: std::error::Error + Send + Sync + 'static,
{
    /// Creates a callable error from a batch execution error and sparse
    /// outputs.
    ///
    /// # Parameters
    ///
    /// * `source` - Batch-level execution error with its partial outcome.
    /// * `outputs` - Successful callable outputs sorted by callable index.
    ///
    /// # Returns
    ///
    /// A callable error preserving both execution metadata and sparse outputs.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) fn new(source: BatchExecutionError<E, S>, outputs: Vec<BatchCallOutput<R>>) -> Self {
        Self {
            source: Box::new(source),
            outputs,
        }
    }

    /// Converts only the scheduler error, preserving all partial callable
    /// outputs.
    ///
    /// # Parameters
    ///
    /// * `map` - Conversion invoked once for ScheduleFailed, never for other
    ///   errors.
    ///
    /// # Returns
    ///
    /// An equivalent error owning the original outputs, outcome and secondary
    /// progress error. Neither successful values nor task errors are cloned.
    ///
    /// # Panics
    ///
    /// Propagates a panic raised by `map`.
    ///
    /// # Type Parameters
    ///
    /// * `S2` - Replacement scheduler error type.
    /// * `F` - One-shot conversion function from `S` to `S2`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_batch::SequentialBatchExecutor;
    /// let error = SequentialBatchExecutor::new()
    ///     .call_with_count([|| Ok::<_, ()>(42)], 2).expect_err("short source");
    /// let mapped = error.map_scheduler_error::<std::io::Error, _>(|never| match never {});
    /// assert_eq!(*mapped.outputs()[0].value(), 42);
    /// assert!(mapped.source().is_count_shortfall());
    /// ```
    pub fn map_scheduler_error<S2, F>(self, map: F) -> BatchCallError<R, E, S2>
    where
        S2: std::error::Error + Send + Sync + 'static,
        F: FnOnce(S) -> S2,
    {
        let (source, outputs) = self.into_parts();
        BatchCallError::new(source.map_scheduler_error(map), outputs)
    }

    /// Returns the nested batch execution error.
    ///
    /// # Returns
    ///
    /// The batch-level error that stopped execution.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn source(&self) -> &BatchExecutionError<E, S> {
        self.source.as_ref()
    }

    /// Returns the partial execution outcome.
    ///
    /// # Returns
    ///
    /// The outcome accumulated before the batch-level error occurred.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn outcome(&self) -> &BatchOutcome<E> {
        self.source.outcome()
    }

    /// Returns successful callable outputs collected before the error.
    ///
    /// # Returns
    ///
    /// Outputs sorted by their original zero-based callable index.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn outputs(&self) -> &[BatchCallOutput<R>] {
        &self.outputs
    }

    /// Consumes this error and returns the nested execution error.
    ///
    /// # Returns
    ///
    /// The owned batch-level execution error.
    #[inline(always)]
    pub fn into_source(self) -> BatchExecutionError<E, S> {
        *self.source
    }

    /// Consumes this error and returns sparse successful callable outputs.
    ///
    /// # Returns
    ///
    /// Successful outputs sorted by their original zero-based callable index.
    #[inline(always)]
    pub fn into_outputs(self) -> Vec<BatchCallOutput<R>> {
        self.outputs
    }

    /// Consumes this error and returns both preserved parts.
    ///
    /// # Returns
    ///
    /// The nested execution error and its preserved successful outputs.
    #[inline(always)]
    pub fn into_parts(self) -> (BatchExecutionError<E, S>, Vec<BatchCallOutput<R>>) {
        (*self.source, self.outputs)
    }
}

impl<R, E, S> fmt::Display for BatchCallError<R, E, S>
where
    S: std::error::Error + Send + Sync + 'static,
{
    /// Formats the nested batch execution error.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Formatter receiving the display representation.
    ///
    /// # Returns
    ///
    /// The formatting result.
    #[inline(always)]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl<R, E, S> std::error::Error for BatchCallError<R, E, S>
where
    E: std::error::Error + 'static,
    S: std::error::Error + Send + Sync + 'static,
{
    /// Returns the nested batch execution error as the source.
    #[inline(always)]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
