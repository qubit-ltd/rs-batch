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
//!     .for_each([1, 2, 3], 3, |value| {
//!         assert!(value > 0);
//!         Ok::<(), &'static str>(())
//!     })
//!     .expect("iterator should yield exactly three items");
//!
//! assert!(outcome.is_success());
//! ```
//!
//! Executor state machines are internal implementation details and are not
//! part of the public crate-root API.
//!
//! ```compile_fail
//! use qubit_batch::BatchExecutionState;
//! ```
//!

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod execute;
pub mod process;
pub(crate) mod utils;

pub use execute::{
    BatchCallResult,
    BatchExecutionError,
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
    BatchProcessor,
    ChunkedBatchProcessError,
    ChunkedBatchProcessor,
    ParallelBatchProcessor,
    SequentialBatchProcessor,
};
