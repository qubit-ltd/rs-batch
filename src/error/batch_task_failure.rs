/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
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
/// # Author
///
/// Haixing Hu
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
    pub fn new(index: usize, error: BatchTaskError<E>) -> Self {
        Self { index, error }
    }

    /// Returns the failed task's zero-based batch index.
    ///
    /// # Returns
    ///
    /// The task index recorded for this failure.
    #[inline]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns the task error recorded for this failure.
    ///
    /// # Returns
    ///
    /// A shared reference to the task error.
    #[inline]
    pub const fn error(&self) -> &BatchTaskError<E> {
        &self.error
    }

    /// Consumes this failure record and returns the stored task error.
    ///
    /// # Returns
    ///
    /// The task error previously stored in this failure record.
    #[inline]
    pub fn into_error(self) -> BatchTaskError<E> {
        self.error
    }
}
