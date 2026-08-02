// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Progress failures normalized for batch APIs.

use qubit_progress::{CompletionError, EmissionError, FinishError, StartError, TerminalError};
use thiserror::Error;

/// Progress failure observed by a batch executor or processor.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum ProgressFailure {
    /// Progress operation could not start.
    #[error("progress operation failed to start")]
    Start(#[source] StartError),
    /// A running event could not be delivered.
    #[error("progress running event failed")]
    Emission(#[source] EmissionError),
    /// A terminal event could not be delivered.
    #[error("progress terminal event failed")]
    Terminal(#[source] TerminalError),
    /// Checked completion found an invalid metric state.
    #[error("progress completion validation failed")]
    Completion(#[source] CompletionError),
}

impl From<StartError> for ProgressFailure {
    /// Wraps a start failure.
    fn from(error: StartError) -> Self {
        Self::Start(error)
    }
}

impl From<EmissionError> for ProgressFailure {
    /// Wraps a running emission failure.
    fn from(error: EmissionError) -> Self {
        Self::Emission(error)
    }
}

impl From<TerminalError> for ProgressFailure {
    /// Wraps a terminal emission failure.
    fn from(error: TerminalError) -> Self {
        Self::Terminal(error)
    }
}

impl ProgressFailure {
    /// Converts a checked finish failure while discarding the unusable
    /// operation returned with an incomplete finish.
    pub fn from_finish_error(error: FinishError<'_>) -> Self {
        match error {
            FinishError::Incomplete { source, .. } => Self::Completion(source),
            FinishError::Terminal(source) => Self::Terminal(source),
        }
    }

    /// Returns terminal elapsed time when this failure attempted a terminal
    /// event.
    #[must_use]
    pub fn elapsed(&self) -> Option<std::time::Duration> {
        match self {
            Self::Terminal(error) => Some(error.elapsed()),
            Self::Start(_) | Self::Emission(_) | Self::Completion(_) => None,
        }
    }
}
