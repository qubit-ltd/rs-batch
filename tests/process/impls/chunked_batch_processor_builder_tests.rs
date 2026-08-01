// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ChunkedBatchProcessorBuilder`](qubit_batch::ChunkedBatchProcessorBuilder).

use std::{num::NonZeroUsize, time::Duration};

use qubit_batch::ChunkedBatchProcessor;

use crate::support::TestChunkProcessor;

#[test]
fn test_chunked_batch_processor_builder_applies_configuration() {
    let processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::success(),
        NonZeroUsize::new(4).expect("chunk size is non-zero"),
    )
    .report_interval(Duration::ZERO)
    .no_reporter()
    .build();

    assert_eq!(processor.chunk_size().get(), 4);
    assert_eq!(processor.report_interval(), Duration::ZERO);
}
