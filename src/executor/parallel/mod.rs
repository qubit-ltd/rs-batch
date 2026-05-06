/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Parallel batch executor implementation.

mod indexed_task;
mod parallel_batch_executor;
mod parallel_batch_executor_build_error;
mod parallel_batch_executor_builder;

pub use parallel_batch_executor::ParallelBatchExecutor;
pub use parallel_batch_executor_build_error::ParallelBatchExecutorBuildError;
pub use parallel_batch_executor_builder::ParallelBatchExecutorBuilder;
