/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::{
    error::Error,
    fmt,
};

/// Error recorded for one task inside a batch execution.
///
/// # Type Parameters
///
/// * `E` - The task-specific error type.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug)]
pub enum BatchTaskError<E> {
    /// The task returned its own business error.
    Failed(E),

    /// The task panicked while running.
    Panicked,
}

impl<E> BatchTaskError<E> {
    /// Returns whether this task error wraps the task's own error value.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::Failed`].
    #[inline]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Returns whether this task error represents a panic.
    ///
    /// # Returns
    ///
    /// `true` if this error is [`Self::Panicked`].
    #[inline]
    pub const fn is_panicked(&self) -> bool {
        matches!(self, Self::Panicked)
    }
}

impl<E> fmt::Display for BatchTaskError<E>
where
    E: fmt::Display,
{
    /// Formats this batch task error for users.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the human-readable message.
    ///
    /// # Returns
    ///
    /// The formatting result produced by `write!`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(error) => write!(f, "task failed: {error}"),
            Self::Panicked => f.write_str("task panicked"),
        }
    }
}

impl<E> Error for BatchTaskError<E> where E: Error + 'static {}
