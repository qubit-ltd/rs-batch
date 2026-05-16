/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
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
//! [`BatchExecutionState`] is public so runtime-specific executor crates can
//! reuse the same accounting and outcome-building rules as the built-in
//! executors.
//!
//! # Progress Interval Semantics
//!
//! Progress reporting has explicit lifecycle events plus optional running
//! events. A `report_interval` is a throttle checked only when an implementation
//! reaches one of its running-progress points; it is not a timer guarantee that
//! a running event is emitted immediately when that duration passes. Passing
//! [`std::time::Duration::ZERO`] disables time throttling, so each
//! implementation-defined running-progress point reports as soon as it is
//! reached. Sequential variants reach those points between tasks or items.
//! Chunked processing reaches them after a chunk completes. Parallel variants
//! report from a scoped reporter thread; with a positive interval they can also
//! emit periodic running events while workers are active, while zero interval
//! reports on worker completion signals and does not spin in a tight loop.
//!

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod execute;
pub mod process;
pub(crate) mod utils;

pub use execute::{
    BatchCallResult,
    BatchExecutionError,
    BatchExecutionState,
    BatchExecutor,
    BatchOutcome,
    BatchOutcomeBuildError,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    ParallelBatchExecutorBuilder,
    SequentialBatchExecutor,
};
pub use process::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessResultBuildError,
    BatchProcessResultBuilder,
    BatchProcessor,
    ChunkedBatchProcessError,
    ChunkedBatchProcessor,
    ParallelBatchProcessor,
    ParallelBatchProcessorBuildError,
    ParallelBatchProcessorBuilder,
    SequentialBatchProcessor,
};
