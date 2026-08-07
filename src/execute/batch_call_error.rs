// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

use crate::{
    BatchExecutionError,
    BatchOutcome,
};

/// Batch-level callable error that preserves successful values collected before
/// execution stopped.
///
/// The nested [`BatchExecutionError`] retains the partial execution outcome and
/// the values slice retains one slot per declared callable. A slot is `Some`
/// only when its callable completed successfully.
///
/// # Type Parameters
///
/// * `R` - Callable success value type.
/// * `E` - Callable error type stored in the nested execution outcome.
#[must_use = "call errors preserve partial callable values"]
pub struct BatchCallError<R, E> {
    source: Box<BatchExecutionError<E>>,
    values: Vec<Option<R>>,
}

impl<R, E> fmt::Debug for BatchCallError<R, E>
where
    E: fmt::Debug,
{
    /// Formats the nested error and the number of preserved values.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchCallError")
            .field("source", &self.source)
            .field("value_count", &self.values.len())
            .finish()
    }
}

impl<R, E> BatchCallError<R, E> {
    /// Creates a callable error from a batch execution error and indexed
    /// values.
    ///
    /// # Parameters
    ///
    /// * `source` - Batch-level execution error with its partial outcome.
    /// * `values` - Callable values indexed by declared task position.
    ///
    /// # Returns
    ///
    /// A callable error preserving both execution metadata and partial values.
    #[inline]
    pub(crate) fn new(
        source: BatchExecutionError<E>,
        values: Vec<Option<R>>,
    ) -> Self {
        Self {
            source: Box::new(source),
            values,
        }
    }

    /// Returns the nested batch execution error.
    #[inline]
    pub fn source(&self) -> &BatchExecutionError<E> {
        self.source.as_ref()
    }

    /// Returns the partial execution outcome.
    #[inline]
    pub fn outcome(&self) -> &BatchOutcome<E> {
        self.source.outcome()
    }

    /// Returns the indexed callable values collected before the error.
    #[inline]
    pub fn values(&self) -> &[Option<R>] {
        &self.values
    }

    /// Consumes this error and returns the nested execution error.
    #[inline]
    pub fn into_source(self) -> BatchExecutionError<E> {
        *self.source
    }

    /// Consumes this error and returns the indexed callable values.
    #[inline]
    pub fn into_values(self) -> Vec<Option<R>> {
        self.values
    }

    /// Consumes this error and returns both preserved parts.
    #[inline]
    pub fn into_parts(self) -> (BatchExecutionError<E>, Vec<Option<R>>) {
        (*self.source, self.values)
    }
}

impl<R, E> fmt::Display for BatchCallError<R, E> {
    /// Formats the nested batch execution error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl<R, E> std::error::Error for BatchCallError<R, E>
where
    E: std::error::Error + 'static,
{
    /// Returns the nested batch execution error as the source.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
