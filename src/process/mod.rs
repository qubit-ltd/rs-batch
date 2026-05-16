/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Batch data processing abstractions, results, and errors.

mod batch_process_error;
mod batch_process_result;
mod batch_process_result_build_error;
mod batch_process_result_builder;
mod batch_process_state;
mod batch_processor;
mod chunked_batch_process_error;
pub mod impls;

pub use batch_process_error::BatchProcessError;
pub use batch_process_result::BatchProcessResult;
pub use batch_process_result_build_error::BatchProcessResultBuildError;
pub use batch_process_result_builder::BatchProcessResultBuilder;
pub(crate) use batch_process_state::BatchProcessState;
pub use batch_processor::BatchProcessor;
pub use chunked_batch_process_error::ChunkedBatchProcessError;
pub use impls::{
    ChunkedBatchProcessor,
    ChunkedBatchProcessorBuilder,
    ParallelBatchProcessor,
    ParallelBatchProcessorBuildError,
    ParallelBatchProcessorBuilder,
    SequentialBatchProcessor,
    SequentialBatchProcessorBuilder,
};
