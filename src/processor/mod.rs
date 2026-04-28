/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Batch data processing abstractions.
//!
//! A processor consumes data items directly. This is separate from
//! [`crate::BatchExecutor`], which executes already-built tasks.
//!
//! # Author
//!
//! Haixing Hu

mod batch_process_result;
mod batch_processor;
mod chunked_batch_process_error;
mod chunked_batch_processor;

pub use batch_process_result::BatchProcessResult;
pub use batch_processor::BatchProcessor;
pub use chunked_batch_process_error::ChunkedBatchProcessError;
pub use chunked_batch_processor::ChunkedBatchProcessor;
