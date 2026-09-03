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
#[must_use = "call errors preserve partial callable values"]
pub struct BatchCallError<R, E, S = Infallible>
where
    S: std::error::Error + Send + Sync + 'static,
{
    source: Box<BatchExecutionError<E, S>>,
    outputs: Vec<BatchCallOutput<R>>,
}

impl<R, E, S> fmt::Debug for BatchCallError<R, E, S>
where
    E: fmt::Debug,
    S: std::error::Error + Send + Sync + 'static,
{
    /// Formats the nested error and the number of preserved outputs.
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
    pub(crate) fn new(source: BatchExecutionError<E, S>, outputs: Vec<BatchCallOutput<R>>) -> Self {
        Self {
            source: Box::new(source),
            outputs,
        }
    }

    /// Returns the nested batch execution error.
    #[inline]
    pub fn source(&self) -> &BatchExecutionError<E, S> {
        self.source.as_ref()
    }

    /// Returns the partial execution outcome.
    #[inline]
    pub fn outcome(&self) -> &BatchOutcome<E> {
        self.source.outcome()
    }

    /// Returns successful callable outputs collected before the error.
    ///
    /// # Returns
    ///
    /// Outputs sorted by their original zero-based callable index.
    #[inline]
    pub fn outputs(&self) -> &[BatchCallOutput<R>] {
        &self.outputs
    }

    /// Consumes this error and returns the nested execution error.
    #[inline]
    pub fn into_source(self) -> BatchExecutionError<E, S> {
        *self.source
    }

    /// Consumes this error and returns sparse successful callable outputs.
    #[inline]
    pub fn into_outputs(self) -> Vec<BatchCallOutput<R>> {
        self.outputs
    }

    /// Consumes this error and returns both preserved parts.
    #[inline]
    pub fn into_parts(self) -> (BatchExecutionError<E, S>, Vec<BatchCallOutput<R>>) {
        (*self.source, self.outputs)
    }
}

impl<R, E, S> fmt::Display for BatchCallError<R, E, S>
where
    S: std::error::Error + Send + Sync + 'static,
{
    /// Formats the nested batch execution error.
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
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
