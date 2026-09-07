// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Batch execution abstractions, outcomes, and task-failure types.

mod batch_call_error;
mod batch_call_output;
mod batch_call_result;
mod batch_call_result_build_error;
mod batch_execution_error;
mod batch_executor;
mod batch_outcome;
mod batch_outcome_build_error;
mod batch_outcome_builder;
mod batch_task_error;
mod batch_task_failure;
mod batch_termination;
pub mod impls;
pub(crate) mod internal;
mod parallel_batch_execution_context;
mod parallel_batch_execution_coordinator;
mod parallel_batch_task;
pub mod spi;
mod task_failure_policy;

pub use batch_call_error::BatchCallError;
pub use batch_call_output::BatchCallOutput;
pub use batch_call_result::BatchCallResult;
pub use batch_call_result_build_error::BatchCallResultBuildError;
pub use batch_execution_error::BatchExecutionError;
pub use batch_executor::BatchExecutor;
pub use batch_outcome::BatchOutcome;
pub use batch_outcome_build_error::BatchOutcomeBuildError;
pub use batch_outcome_builder::BatchOutcomeBuilder;
pub use batch_task_error::BatchTaskError;
pub(crate) use batch_task_error::panic_payload_to_error;
pub use batch_task_failure::BatchTaskFailure;
pub use batch_termination::BatchTermination;
pub use impls::ParallelBatchExecutor;
pub use impls::ParallelBatchExecutorBuildError;
pub use impls::ParallelBatchExecutorBuilder;
pub use impls::SequentialBatchExecutor;
pub use impls::SequentialBatchExecutorBuilder;
pub(crate) use internal::BatchExecutionState;
pub(crate) use internal::EXECUTION_PROGRESS_METRIC_ID;
pub(crate) use internal::EXECUTION_PROGRESS_METRIC_NAME;
pub(crate) use internal::TaskExecutionStatus;
pub(crate) use parallel_batch_execution_context::ParallelBatchExecutionContext;
pub(crate) use parallel_batch_execution_coordinator::ParallelBatchExecutionCoordinator;
pub(crate) use parallel_batch_task::ParallelBatchTask;
pub use task_failure_policy::TaskFailurePolicy;
