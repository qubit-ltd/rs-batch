/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Batch execution error types and structured results.

mod batch_execution_error;
mod batch_execution_result;
mod batch_execution_result_build_error;
mod batch_task_error;
mod batch_task_failure;

pub use batch_execution_error::BatchExecutionError;
pub use batch_execution_result::BatchExecutionResult;
pub use batch_execution_result_build_error::BatchExecutionResultBuildError;
pub use batch_task_error::BatchTaskError;
pub(crate) use batch_task_error::panic_payload_to_error;
pub use batch_task_failure::BatchTaskFailure;
