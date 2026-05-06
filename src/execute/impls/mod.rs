/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Standard-library implementations of batch execution traits.

mod indexed_task;
mod parallel_batch_executor;
mod parallel_batch_executor_build_error;
mod parallel_batch_executor_builder;
mod sequential_batch_executor;

pub use parallel_batch_executor::ParallelBatchExecutor;
pub use parallel_batch_executor_build_error::ParallelBatchExecutorBuildError;
pub use parallel_batch_executor_builder::ParallelBatchExecutorBuilder;
pub use sequential_batch_executor::SequentialBatchExecutor;
