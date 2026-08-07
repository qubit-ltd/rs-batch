// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_progress::MetricError;
use thiserror::Error;

/// Error returned when a task cannot be recorded in execution context.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ParallelBatchExecutionContextError {
    /// The progress metric rejected an execution lifecycle transition.
    #[error("batch execution progress metric update failed: {0}")]
    ProgressMetric(#[from] MetricError),
    /// The supplied task index is outside the declared batch range.
    #[error(
        "batch task index {index} is outside the declared task count {task_count}"
    )]
    TaskIndexOutOfRange {
        /// Supplied zero-based task index.
        index: usize,
        /// Declared number of tasks in the batch.
        task_count: usize,
    },
}
