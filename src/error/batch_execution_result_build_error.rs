/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use thiserror::Error;

/// Error returned when constructing a batch execution result with invalid
/// counters or failure details.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchExecutionResultBuildError {
    /// The completed task count is greater than the declared task count.
    #[error(
        "completed task count must not exceed declared task count: task_count {task_count}, completed_count {completed_count}"
    )]
    CompletedCountExceeded {
        /// Declared task count.
        task_count: usize,
        /// Number of completed tasks.
        completed_count: usize,
    },

    /// Adding failed and panicked task counts overflowed.
    #[error(
        "failed and panicked task counts must not overflow: failed_count {failed_count}, panicked_count {panicked_count}"
    )]
    FailureCountOverflow {
        /// Number of tasks that returned their own error.
        failed_count: usize,
        /// Number of tasks that panicked.
        panicked_count: usize,
    },

    /// Adding successful and failed task counts overflowed.
    #[error(
        "terminal task counts must not overflow: succeeded_count {succeeded_count}, failure_count {failure_count}"
    )]
    TerminalCountOverflow {
        /// Number of successful tasks.
        succeeded_count: usize,
        /// Number of failed or panicked tasks.
        failure_count: usize,
    },

    /// Completed tasks do not equal successful plus failed plus panicked tasks.
    #[error(
        "completed task count must equal succeeded + failed + panicked counts: completed_count {completed_count}, terminal_count {terminal_count}"
    )]
    TerminalCountMismatch {
        /// Number of completed tasks.
        completed_count: usize,
        /// Number of successful, failed, and panicked tasks combined.
        terminal_count: usize,
        /// Number of successful tasks.
        succeeded_count: usize,
        /// Number of tasks that returned their own error.
        failed_count: usize,
        /// Number of tasks that panicked.
        panicked_count: usize,
    },

    /// Detailed failure records do not match the aggregate failure count.
    #[error(
        "failure detail count must equal failed + panicked counts: expected {expected}, actual {actual}"
    )]
    FailureDetailCountMismatch {
        /// Expected number of failure details.
        expected: usize,
        /// Actual number of failure details.
        actual: usize,
    },

    /// A failure detail index is outside the declared task range.
    #[error(
        "failure index must be less than declared task count: index {index}, task_count {task_count}"
    )]
    FailureIndexOutOfRange {
        /// Out-of-range failure index.
        index: usize,
        /// Declared task count.
        task_count: usize,
    },

    /// Detailed failure variants do not match `failed_count` and
    /// `panicked_count`.
    #[error(
        "failure detail variants must match failed_count and panicked_count: expected_failed {expected_failed}, actual_failed {actual_failed}, expected_panicked {expected_panicked}, actual_panicked {actual_panicked}"
    )]
    FailureVariantCountMismatch {
        /// Expected number of business failure details.
        expected_failed: usize,
        /// Actual number of business failure details.
        actual_failed: usize,
        /// Expected number of panic failure details.
        expected_panicked: usize,
        /// Actual number of panic failure details.
        actual_panicked: usize,
    },
}
