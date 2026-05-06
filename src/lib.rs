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

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod execution;
pub mod executor;
pub mod processor;

pub use error::{
    BatchExecutionError,
    BatchTaskError,
    BatchTaskFailure,
};
pub use execution::{
    BatchExecutionState,
    BatchOutcome,
    BatchOutcomeBuildError,
};
pub use executor::{
    BatchCallResult,
    BatchExecutor,
    SequentialBatchExecutor,
};
pub use processor::{
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessError,
    ChunkedBatchProcessor,
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
