// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::batch_task_error::BatchTaskError;

/// Failure record for one task inside a batch.
///
/// Each failure keeps the task's stable batch index so callers can map the
/// failure back to the source task.
///
/// # Type Parameters
///
/// * `E` - The task-specific error type.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::{
///     BatchTaskError,
///     BatchTaskFailure,
/// };
///
/// let failure = BatchTaskFailure::new(2, BatchTaskError::Failed("invalid row"));
///
/// assert_eq!(failure.index(), 2);
/// assert!(failure.error().is_failed());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchTaskFailure<E> {
    /// Zero-based task index within the batch.
    index: usize,
    /// Error observed for this task.
    error: BatchTaskError<E>,
}

impl<E> BatchTaskFailure<E> {
    /// Creates a new batch task failure record.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index within the batch.
    /// * `error` - Error observed for the task at `index`.
    ///
    /// # Returns
    ///
    /// A failure record containing the task index and error.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub fn new(index: usize, error: BatchTaskError<E>) -> Self {
        Self { index, error }
    }

    /// Returns the failed task's zero-based batch index.
    ///
    /// # Returns
    ///
    /// The task index recorded for this failure.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns the task error recorded for this failure.
    ///
    /// # Returns
    ///
    /// A shared reference to the task error.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn error(&self) -> &BatchTaskError<E> {
        &self.error
    }

    /// Consumes this failure record and returns the stored task error.
    ///
    /// # Returns
    ///
    /// The task error previously stored in this failure record.
    #[inline(always)]
    pub fn into_error(self) -> BatchTaskError<E> {
        self.error
    }
}
