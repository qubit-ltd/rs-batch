/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::{
    BatchProcessResult,
    ChunkedBatchProcessError,
};
use std::time::Duration;

#[test]
fn test_chunked_batch_process_error_result_accessors() {
    let result = BatchProcessResult::builder(4)
        .completed_count(2)
        .processed_count(2)
        .chunk_count(2)
        .elapsed(Duration::from_millis(10))
        .build()
        .expect("process result counters should be valid");
    let error = ChunkedBatchProcessError::ChunkFailed {
        chunk_index: 1,
        start_index: 2,
        chunk_len: 2,
        source: "failed",
        result: result.clone(),
    };

    assert_eq!(error.result(), &result);
    assert!(error.to_string().contains("batch chunk 1 failed"));
    assert_eq!(error.into_result(), result);
}
