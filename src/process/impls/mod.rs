/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Standard-library implementations of batch processing traits.

mod chunked_batch_processor;
mod parallel_batch_processor;
mod parallel_batch_processor_builder;
mod sequential_batch_processor;

pub use chunked_batch_processor::ChunkedBatchProcessor;
pub use parallel_batch_processor::ParallelBatchProcessor;
pub use parallel_batch_processor_builder::ParallelBatchProcessorBuilder;
pub use sequential_batch_processor::SequentialBatchProcessor;
