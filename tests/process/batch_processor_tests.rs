/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_batch::{
    BatchProcessor,
    ChunkedBatchProcessor,
    SequentialBatchProcessor,
};

use crate::support::TestChunkProcessor;

#[test]
fn test_batch_processor_chunked_accessors() {
    let processor = ChunkedBatchProcessor::new(
        TestChunkProcessor::success(),
        NonZeroUsize::new(3).expect("non-zero chunk size"),
    )
    .with_report_interval(Duration::from_millis(25));

    assert_eq!(processor.chunk_size().get(), 3);
    assert_eq!(processor.report_interval(), Duration::from_millis(25));
}

#[test]
fn test_batch_processor_trait_process_derives_count_from_exact_iterator() {
    let mut processor = SequentialBatchProcessor::new(|_item: &i32| {});

    let result = processor
        .process([1, 2, 3])
        .expect("array length should be exact");

    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.processed_count(), 3);
}
