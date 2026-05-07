/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    any::Any,
    error::Error,
    fmt,
};

/// Error recorded for one task inside a batch execution.
///
/// Use this type to distinguish a task's returned business error from a panic
/// captured while running that task.
///
/// ```rust
/// use qubit_batch::BatchTaskError;
///
/// let failed = BatchTaskError::Failed("invalid record");
/// assert!(failed.is_failed());
/// assert_eq!(failed.panic_message(), None);
///
/// let panicked: BatchTaskError<&'static str> = BatchTaskError::panicked("boom");
/// assert!(panicked.is_panicked());
/// assert_eq!(panicked.panic_message(), Some("boom"));
/// ```
///
/// # Type Parameters
///
/// * `E` - The task-specific error type.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchTaskError<E> {
    /// The task returned its own business error.
    Failed(E),

    /// The task panicked while running.
    Panicked {
        /// Captured panic message when the panic payload is a string.
        message: Option<String>,
    },
}

impl<E> BatchTaskError<E> {
    /// Creates a panicked task error from a captured panic payload.
    ///
    /// # Parameters
    ///
    /// * `payload` - Panic payload captured by `catch_unwind`.
    ///
    /// # Returns
    ///
    /// A panicked task error containing a string message when the payload carries
    /// one.
    #[inline]
    pub fn from_panic_payload(payload: &(dyn Any + Send)) -> Self {
        match panic_payload_message(payload) {
            Some(message) => Self::panicked(message),
            None => Self::panicked_without_message(),
        }
    }

    /// Creates a panicked task error with a captured message.
    ///
    /// # Parameters
    ///
    /// * `message` - Panic message captured from the task.
    ///
    /// # Returns
    ///
    /// A panicked task error containing `message`.
    #[inline]
    pub fn panicked(message: impl Into<String>) -> Self {
        Self::Panicked {
            message: Some(message.into()),
        }
    }

    /// Creates a panicked task error without a readable message.
    ///
    /// # Returns
    ///
    /// A panicked task error with no captured message.
    #[inline]
    pub const fn panicked_without_message() -> Self {
        Self::Panicked { message: None }
    }

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
        matches!(self, Self::Panicked { .. })
    }

    /// Returns the captured panic message, if one is available.
    ///
    /// # Returns
    ///
    /// `Some(message)` when the panic payload was a string, or `None` for
    /// business errors and non-string panic payloads.
    #[inline]
    pub fn panic_message(&self) -> Option<&str> {
        match self {
            Self::Failed(_) | Self::Panicked { message: None } => None,
            Self::Panicked {
                message: Some(message),
            } => Some(message.as_str()),
        }
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
            Self::Panicked { message: None } => f.write_str("task panicked"),
            Self::Panicked {
                message: Some(message),
            } => write!(f, "task panicked: {message}"),
        }
    }
}

impl<E> Error for BatchTaskError<E>
where
    E: Error + 'static,
{
    /// Returns the wrapped task error as the source when this error represents
    /// a business failure.
    ///
    /// # Returns
    ///
    /// `Some(error)` for [`Self::Failed`], or `None` for task panics because a
    /// panic payload is not an error source.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Panicked { .. } => None,
        }
    }
}

/// Converts a panic payload into a task panic error.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// A panicked task error containing a string message when the payload carries
/// one.
pub(crate) fn panic_payload_to_error<E>(payload: &(dyn Any + Send)) -> BatchTaskError<E> {
    BatchTaskError::from_panic_payload(payload)
}

/// Extracts a readable panic message from a panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// A cloned panic message when `payload` is `&'static str` or `String`.
fn panic_payload_message(payload: &(dyn Any + Send)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}
