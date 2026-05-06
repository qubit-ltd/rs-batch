/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Batch data processing abstractions.
//!
//! A processor consumes data items directly. This is separate from
//! [`crate::BatchExecutor`], which executes already-built tasks.
//!

mod batch_process_error;
mod batch_process_result;
mod batch_processor;
mod chunked_batch_process_error;
mod chunked_batch_processor;
mod parallel_batch_processor;
mod sequential_batch_processor;

pub use batch_process_error::BatchProcessError;
pub use batch_process_result::BatchProcessResult;
pub use batch_processor::BatchProcessor;
pub use chunked_batch_process_error::ChunkedBatchProcessError;
pub use chunked_batch_processor::ChunkedBatchProcessor;
pub use parallel_batch_processor::ParallelBatchProcessor;
pub use sequential_batch_processor::SequentialBatchProcessor;
