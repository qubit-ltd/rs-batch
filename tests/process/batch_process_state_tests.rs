/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Behavioral coverage for process state accounting.

use std::num::NonZeroUsize;

use qubit_batch::{
    BatchProcessor,
    ChunkedBatchProcessor,
    SequentialBatchProcessor,
};

#[test]
fn test_batch_process_state_builds_direct_and_chunked_results() {
    let mut direct = SequentialBatchProcessor::new(|_item: &usize| {});
    let direct_result = direct
        .process([1usize, 2usize, 3usize], 3)
        .expect("direct processing should succeed");

    let delegate = SequentialBatchProcessor::new(|_item: &usize| {});
    let mut chunked = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );
    let chunked_result = chunked
        .process([1usize, 2usize, 3usize], 3)
        .expect("chunked processing should succeed");

    assert_eq!(direct_result.completed_count(), 3);
    assert_eq!(direct_result.processed_count(), 3);
    assert_eq!(direct_result.chunk_count(), 1);
    assert_eq!(chunked_result.completed_count(), 3);
    assert_eq!(chunked_result.processed_count(), 3);
    assert_eq!(chunked_result.chunk_count(), 2);
}
