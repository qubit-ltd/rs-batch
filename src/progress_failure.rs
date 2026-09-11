// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Progress failures normalized for batch APIs.

use std::time::Duration;

use qubit_progress::AutoReporterError;
use qubit_progress::CompletionError;
use qubit_progress::EmissionError;
use qubit_progress::FinishError;
use qubit_progress::Progress;
use qubit_progress::StartError;
use qubit_progress::TerminalError;
use thiserror::Error;

/// Progress failure observed by a batch executor or processor.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::ProgressFailure;
/// use qubit_progress::Metric;
/// use qubit_progress::NoopReporter;
/// use qubit_progress::Progress;
/// let reporter = NoopReporter;
/// let progress = Progress::builder(&reporter)
///     .metric(Metric::new("items", "Items").total(1)).start().expect("valid metric");
/// let error = progress.finish().expect_err("one item remains incomplete");
/// assert!(matches!(ProgressFailure::from_finish_error(error), ProgressFailure::Completion(_)));
/// ```
#[must_use = "progress failures preserve the failed lifecycle operation"]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProgressFailure {
    /// Progress operation could not start.
    #[error("progress operation failed to start")]
    Start(
        /// Underlying progress error retaining its source and lifecycle
        /// context.
        #[source]
        StartError,
    ),
    /// A running event could not be delivered.
    #[error("progress running event failed")]
    Emission(
        /// Underlying progress error retaining its source and lifecycle
        /// context.
        #[source]
        EmissionError,
    ),
    /// A scoped automatic reporter stopped with an emission error or panic.
    #[error("progress automatic reporter failed")]
    AutoReporter(
        /// Underlying progress error retaining its source and lifecycle
        /// context.
        #[source]
        AutoReporterError,
    ),
    /// A terminal event could not be delivered.
    #[error("progress terminal event failed")]
    Terminal(
        /// Underlying progress error retaining its source and lifecycle
        /// context.
        #[source]
        TerminalError,
    ),
    /// Checked completion found an invalid metric state.
    #[error("progress completion validation failed")]
    Completion(
        /// Underlying progress error retaining its source and lifecycle
        /// context.
        #[source]
        CompletionError,
    ),
}

impl From<StartError> for ProgressFailure {
    /// Wraps a start failure.
    #[inline(always)]
    fn from(error: StartError) -> Self {
        Self::Start(error)
    }
}

impl From<EmissionError> for ProgressFailure {
    /// Wraps a running emission failure.
    #[inline(always)]
    fn from(error: EmissionError) -> Self {
        Self::Emission(error)
    }
}

impl From<AutoReporterError> for ProgressFailure {
    /// Wraps a scoped automatic reporter failure.
    #[inline(always)]
    fn from(error: AutoReporterError) -> Self {
        Self::AutoReporter(error)
    }
}

impl From<TerminalError> for ProgressFailure {
    /// Wraps a terminal emission failure.
    #[inline(always)]
    fn from(error: TerminalError) -> Self {
        Self::Terminal(error)
    }
}

impl ProgressFailure {
    /// Converts a checked finish failure while discarding the unusable
    /// operation returned with an incomplete finish.
    ///
    /// # Parameters
    ///
    /// * `error` - Checked finish failure returned by the progress operation.
    ///
    /// # Returns
    ///
    /// A normalized progress failure retaining the underlying source error.
    pub fn from_finish_error(error: FinishError) -> Self {
        match error {
            FinishError::Incomplete { source, .. } => Self::Completion(source),
            FinishError::Terminal(source) => Self::Terminal(source),
        }
    }

    /// Returns terminal elapsed time when this failure attempted a terminal
    /// event.
    ///
    /// # Returns
    ///
    /// The terminal event's elapsed duration, or `None` when no terminal event
    /// was attempted.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn elapsed(&self) -> Option<Duration> {
        match self {
            Self::Terminal(error) => Some(error.elapsed()),
            Self::Start(_) | Self::Emission(_) | Self::AutoReporter(_) | Self::Completion(_) => {
                None
            }
        }
    }

    /// Sends a failed terminal event while retaining a secondary reporter
    /// error.
    ///
    /// # Parameters
    ///
    /// * `progress` - Active operation consumed by its failed terminal event.
    ///
    /// # Returns
    ///
    /// Terminal elapsed time and `None` on successful delivery, or `Some`
    /// containing the delivery error. Callers retain their primary batch error.
    ///
    /// # Panics
    ///
    /// Propagates a panic from the synchronous terminal reporter callback.
    pub(crate) fn fail_operation(progress: Progress<'_>) -> (Duration, Option<Self>) {
        match progress.fail() {
            Ok(elapsed) => (elapsed, None),
            Err(source) => (source.elapsed(), Some(Self::from(source))),
        }
    }
}
