// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Batch execution abstractions, outcomes, and task-failure types.

mod batch_call_result;
mod batch_call_result_build_error;
mod batch_execution_error;
mod batch_execution_state;
mod batch_execution_state_error;
mod batch_executor;
mod batch_outcome;
mod batch_outcome_build_error;
mod batch_outcome_builder;
mod batch_task_error;
mod batch_task_failure;
mod batch_termination;
mod callable_task;
mod for_each_task;
mod parallel_batch_execution;
mod parallel_batch_execution_context;
pub mod impls;
mod task_failure_policy;

pub use batch_call_result::BatchCallResult;
pub use batch_call_result_build_error::BatchCallResultBuildError;
pub use batch_execution_error::BatchExecutionError;
pub use batch_execution_state::{
    BatchExecutionState, EXECUTION_PROGRESS_METRIC_ID, EXECUTION_PROGRESS_METRIC_NAME,
};
pub use batch_execution_state_error::BatchExecutionStateError;
pub use batch_executor::BatchExecutor;
pub use batch_outcome::BatchOutcome;
pub use batch_outcome_build_error::BatchOutcomeBuildError;
pub use batch_outcome_builder::BatchOutcomeBuilder;
pub use batch_task_error::BatchTaskError;
pub(crate) use batch_task_error::panic_payload_to_error;
pub use batch_task_failure::BatchTaskFailure;
pub use batch_termination::BatchTermination;
pub use parallel_batch_execution::ParallelBatchExecution;
pub use parallel_batch_execution_context::ParallelBatchExecutionContext;
pub use impls::{
    ParallelBatchExecutor, ParallelBatchExecutorBuildError, ParallelBatchExecutorBuilder,
    SequentialBatchExecutor, SequentialBatchExecutorBuilder,
};
pub use task_failure_policy::TaskFailurePolicy;
