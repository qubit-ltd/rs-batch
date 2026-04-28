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

pub mod error;
pub mod executor;
pub mod progress;

pub use error::{
    BatchExecutionError,
    BatchExecutionResult,
    BatchExecutionResultBuildError,
    BatchTaskError,
    BatchTaskFailure,
};
pub use executor::{
    BatchExecutor,
    SequentialBatchExecutor,
};
pub use progress::{
    NoOpProgressReporter,
    ProgressReporter,
};
