// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Batch data processing abstractions, results, and errors.

mod batch_process_error;
mod batch_process_result;
mod batch_process_result_build_error;
mod batch_process_result_builder;
mod batch_processor;
mod chunked_batch_process_error;
pub mod impls;
mod internal;

pub use batch_process_error::BatchProcessError;
pub use batch_process_result::BatchProcessResult;
pub use batch_process_result_build_error::BatchProcessResultBuildError;
pub use batch_process_result_builder::BatchProcessResultBuilder;
pub use batch_processor::BatchProcessor;
pub use chunked_batch_process_error::ChunkedBatchProcessError;
pub use impls::ChunkedBatchProcessor;
pub use impls::ChunkedBatchProcessorBuilder;
pub use impls::ParallelBatchProcessor;
pub use impls::ParallelBatchProcessorBuildError;
pub use impls::ParallelBatchProcessorBuilder;
pub use impls::SequentialBatchProcessor;
pub use impls::SequentialBatchProcessorBuilder;
pub(crate) use internal::BatchProcessState;
pub(crate) use internal::PROCESS_PROGRESS_METRIC_ID;
pub(crate) use internal::PROCESS_PROGRESS_METRIC_NAME;
