/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Batch executor traits and implementations.
//!

mod batch_call_result;
mod batch_executor;
mod callable_task;
mod for_each_task;
mod parallel;
mod sequential_batch_executor;

pub use batch_call_result::BatchCallResult;
pub use batch_executor::BatchExecutor;
pub use parallel::{
    ParallelBatchExecutor, ParallelBatchExecutorBuildError, ParallelBatchExecutorBuilder,
};
pub use sequential_batch_executor::SequentialBatchExecutor;
