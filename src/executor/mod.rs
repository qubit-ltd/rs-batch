/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Batch executor traits and implementations.
//!
//! # Author
//!
//! Haixing Hu

mod batch_executor;
mod parallel_batch_executor;
mod parallel_batch_executor_builder;
mod sequential_batch_executor;

pub use batch_executor::BatchExecutor;
pub use parallel_batch_executor::{
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
};
pub use parallel_batch_executor_builder::ParallelBatchExecutorBuilder;
pub use sequential_batch_executor::SequentialBatchExecutor;
