// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Error returned when sparse callable outputs do not match their execution
/// outcome.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchCallResultBuildError {
    /// The number of sparse outputs differs from the successful task count.
    #[error(
        "successful callable output count must equal succeeded task count: succeeded_count {succeeded_count}, output_count {output_count}"
    )]
    SucceededOutputCountMismatch {
        /// Number of successful callable tasks in the execution outcome.
        succeeded_count: usize,
        /// Number of supplied sparse outputs.
        output_count: usize,
    },

    /// An output index is not strictly greater than the previous index.
    #[error(
        "callable output indexes must be strictly increasing: previous {previous_index}, current {index}"
    )]
    OutputIndexOutOfOrder {
        /// Previous output index.
        previous_index: usize,
        /// Current output index.
        index: usize,
    },

    /// An output refers to a task that did not complete.
    #[error(
        "callable output index {index} is not completed: completed_count {completed_count}"
    )]
    OutputIndexNotCompleted {
        /// Original zero-based callable index.
        index: usize,
        /// Number of completed tasks.
        completed_count: usize,
    },

    /// A failed or panicked callable still contains a success output.
    #[error(
        "failed or panicked callable at index {index} must not contain a value"
    )]
    FailureOutputPresent {
        /// Original zero-based callable index.
        index: usize,
    },
}
