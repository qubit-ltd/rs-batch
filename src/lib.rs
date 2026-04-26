/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Batch-oriented task execution utilities.
//!
//! This crate focuses on one-shot execution of whole task batches rather than
//! single-task submission services.
//!
//! # Author
//!
//! Haixing Hu

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

mod batch_execution_error;
mod batch_execution_result;
mod batch_task_error;
mod batch_task_failure;
pub mod executor;
pub mod progress;

pub use batch_execution_error::BatchExecutionError;
pub use batch_execution_result::{
    BatchExecutionResult,
    BatchExecutionResultBuildError,
};
pub use batch_task_error::BatchTaskError;
pub use batch_task_failure::BatchTaskFailure;
pub use executor::{
    BatchExecutor,
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    ParallelBatchExecutorBuilder,
    SequentialBatchExecutor,
};
pub use progress::{
    NoOpProgressReporter,
    ProgressReporter,
};
