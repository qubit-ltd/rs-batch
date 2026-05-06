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
//! Executor state machines are internal implementation details and are not
//! part of the public crate-root API.
//!
//! ```compile_fail
//! use qubit_batch::BatchExecutionState;
//! ```
//!

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod execution;
pub mod executor;
pub mod processor;
pub(crate) mod runtime;
pub(crate) mod state;

pub use error::{
    BatchExecutionError,
    BatchTaskError,
    BatchTaskFailure,
};
pub use execution::{
    BatchOutcome,
    BatchOutcomeBuildError,
    BatchOutcomeBuilder,
};
pub use executor::{
    BatchCallResult,
    BatchExecutor,
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    ParallelBatchExecutorBuilder,
    SequentialBatchExecutor,
};
pub use processor::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessError,
    ChunkedBatchProcessor,
    ParallelBatchProcessor,
    SequentialBatchProcessor,
};
pub use qubit_progress::{
    model::{
        ProgressCounters,
        ProgressEvent,
        ProgressPhase,
        ProgressStage,
    },
    reporter::{
        LoggerProgressReporter,
        NoOpProgressReporter,
        ProgressReporter,
        WriterProgressReporter,
    },
};
