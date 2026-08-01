// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Error returned when callable values do not match their execution outcome.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchCallResultBuildError {
    /// The value vector length differs from the declared callable count.
    #[error(
        "callable value count must equal declared task count: task_count {task_count}, value_count {value_count}"
    )]
    ValueCountMismatch {
        /// Declared callable count from the execution outcome.
        task_count: usize,
        /// Number of supplied callable values.
        value_count: usize,
    },

    /// The number of present values differs from the successful task count.
    #[error(
        "successful callable value count must equal succeeded task count: succeeded_count {succeeded_count}, value_count {value_count}"
    )]
    SucceededValueCountMismatch {
        /// Number of successful callable tasks in the execution outcome.
        succeeded_count: usize,
        /// Number of present callable values.
        value_count: usize,
    },

    /// A failed or panicked callable still contains a success value.
    #[error("failed or panicked callable at index {index} must not contain a value")]
    FailureValuePresent {
        /// Original zero-based callable index.
        index: usize,
    },
}
