// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
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
}
