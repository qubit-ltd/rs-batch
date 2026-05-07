/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Batch execution abstractions, outcomes, and task-failure types.

mod batch_call_result;
mod batch_execution_error;
mod batch_execution_state;
mod batch_executor;
mod batch_outcome;
mod batch_outcome_build_error;
mod batch_outcome_builder;
mod batch_task_error;
mod batch_task_failure;
mod callable_task;
mod for_each_task;
pub mod impls;

pub use batch_call_result::BatchCallResult;
pub use batch_execution_error::BatchExecutionError;
pub use batch_execution_state::BatchExecutionState;
pub use batch_executor::BatchExecutor;
pub use batch_outcome::BatchOutcome;
pub use batch_outcome_build_error::BatchOutcomeBuildError;
pub use batch_outcome_builder::BatchOutcomeBuilder;
pub use batch_task_error::BatchTaskError;
pub(crate) use batch_task_error::panic_payload_to_error;
pub use batch_task_failure::BatchTaskFailure;
pub use impls::{
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    ParallelBatchExecutorBuilder,
    SequentialBatchExecutor,
};
