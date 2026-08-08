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
//! [`execute::spi::ParallelBatchExecutionCoordinator`] and
//! [`execute::spi::ParallelBatchExecutionContext`] let runtime-specific
//! executor crates reuse the built-in progress,
//! accounting, and outcome rules while supplying only their scheduler.
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

#![cfg_attr(doctest, doc = include_str!("../README.md"))]
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(clippy::result_large_err)]

pub mod execute;
pub mod process;
pub(crate) mod utils;

mod constants;
mod progress_failure;
mod sync;

pub use execute::BatchCallError;
pub use execute::BatchCallOutput;
pub use execute::BatchCallResult;
pub use execute::BatchCallResultBuildError;
pub use execute::BatchExecutionError;
pub use execute::BatchExecutor;
pub use execute::BatchOutcome;
pub use execute::BatchOutcomeBuildError;
pub use execute::BatchOutcomeBuilder;
pub use execute::BatchTaskError;
pub use execute::BatchTaskFailure;
pub use execute::BatchTermination;
pub use execute::ParallelBatchExecutor;
pub use execute::ParallelBatchExecutorBuildError;
pub use execute::ParallelBatchExecutorBuilder;
pub use execute::SequentialBatchExecutor;
pub use execute::SequentialBatchExecutorBuilder;
pub use execute::TaskFailurePolicy;
pub use process::BatchProcessError;
pub use process::BatchProcessResult;
pub use process::BatchProcessResultBuildError;
pub use process::BatchProcessResultBuilder;
pub use process::BatchProcessor;
pub use process::ChunkedBatchProcessError;
pub use process::ChunkedBatchProcessor;
pub use process::ChunkedBatchProcessorBuilder;
pub use process::ParallelBatchProcessor;
pub use process::ParallelBatchProcessorBuildError;
pub use process::ParallelBatchProcessorBuilder;
pub use process::SequentialBatchProcessor;
pub use process::SequentialBatchProcessorBuilder;
pub use progress_failure::ProgressFailure;
