// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Implementation state and adapters owned by execute.

mod batch_execution_state;
mod callable_task;
mod for_each_task;
mod parallel_batch_acceptance_state;
mod task_execution_status;

pub(crate) use batch_execution_state::BatchExecutionState;
pub(crate) use batch_execution_state::EXECUTION_PROGRESS_METRIC_ID;
pub(crate) use batch_execution_state::EXECUTION_PROGRESS_METRIC_NAME;
pub(crate) use callable_task::CallableTask;
pub(crate) use for_each_task::ForEachTask;
pub(crate) use parallel_batch_acceptance_state::ParallelBatchAcceptanceState;
pub(crate) use task_execution_status::TaskExecutionStatus;
