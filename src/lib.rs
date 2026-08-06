// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Batch-oriented task execution utilities.
//!
//! This crate focuses on one-shot execution of whole task batches rather than
//! single-task submission services.
//!
//! Core types are re-exported from the crate root, so callers can import the
//! executor trait, result type, and concrete implementation together.
//!
//! ```rust
//! use qubit_batch::{
//!     BatchExecutor,
//!     BatchOutcome,
//!     SequentialBatchExecutor,
//! };
//!
//! let outcome: BatchOutcome<&'static str> = SequentialBatchExecutor::new()
//!     .for_each([1, 2, 3], |value| {
//!         assert!(value > 0);
//!         Ok::<(), &'static str>(())
//!     })
//!     .expect("array length should be exact");
//!
//! assert!(outcome.is_success());
//! ```
//!
//! [`ParallelBatchExecution`] and [`ParallelBatchExecutionContext`] let
//! runtime-specific executor crates reuse the built-in progress, accounting,
//! and outcome rules while supplying only their scheduler.
//!
//! # Progress Interval Semantics
//!
//! Progress reporting has explicit lifecycle events plus optional running
//! events. A `report_interval` is a throttle checked only when an
//! implementation reaches one of its running-progress points; it is not a timer
//! guarantee that a running event is emitted immediately when that duration
//! passes. Passing [`std::time::Duration::ZERO`] disables time throttling, so
//! each implementation-defined running-progress point reports as soon as it is
//! reached. Sequential variants reach those points between tasks or items.
//! Chunked processing reaches them after a chunk completes. Parallel variants
//! report from a scoped reporter thread; with a positive interval they can also
//! emit periodic running events while workers are active, while zero interval
//! reports on worker completion signals and does not spin in a tight loop.

#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod execute;
pub mod process;
pub(crate) mod utils;

mod progress_failure;

pub use progress_failure::ProgressFailure;

pub use execute::{
    BatchCallResult, BatchCallResultBuildError, BatchExecutionError, BatchExecutionState,
    BatchExecutionStateError, BatchExecutor, BatchOutcome, BatchOutcomeBuildError,
    BatchOutcomeBuilder, BatchTaskError, BatchTaskFailure, BatchTermination,
    EXECUTION_PROGRESS_METRIC_ID, EXECUTION_PROGRESS_METRIC_NAME, ParallelBatchExecutor,
    ParallelBatchExecution, ParallelBatchExecutionContext, ParallelBatchExecutorBuildError,
    ParallelBatchExecutorBuilder, SequentialBatchExecutor,
    SequentialBatchExecutorBuilder, TaskFailurePolicy,
};
pub use process::{
    BatchProcessError, BatchProcessResult, BatchProcessResultBuildError, BatchProcessResultBuilder,
    BatchProcessor, ChunkedBatchProcessError, ChunkedBatchProcessor, ChunkedBatchProcessorBuilder,
    ParallelBatchProcessor, ParallelBatchProcessorBuildError, ParallelBatchProcessorBuilder,
    SequentialBatchProcessor, SequentialBatchProcessorBuilder,
};
