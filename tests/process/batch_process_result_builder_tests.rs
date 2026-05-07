/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`BatchProcessResultBuilder`](qubit_batch::BatchProcessResultBuilder).

use std::time::Duration;

use qubit_batch::{
    BatchProcessResultBuildError,
    BatchProcessResultBuilder,
};

#[test]
fn test_batch_process_result_builder_builds_valid_result() {
    let result = BatchProcessResultBuilder::builder(4)
        .completed_count(3)
        .processed_count(2)
        .chunk_count(2)
        .elapsed(Duration::from_millis(15))
        .build()
        .expect("valid process result counters should build");

    assert_eq!(result.item_count(), 4);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(result.chunk_count(), 2);
    assert_eq!(result.elapsed(), Duration::from_millis(15));
    assert!(!result.is_success());
}

#[test]
fn test_batch_process_result_builder_rejects_invalid_counters() {
    assert!(matches!(
        BatchProcessResultBuilder::builder(2)
            .completed_count(3)
            .processed_count(3)
            .chunk_count(1)
            .build(),
        Err(BatchProcessResultBuildError::CompletedCountExceeded {
            item_count: 2,
            completed_count: 3,
        })
    ));
    assert!(matches!(
        BatchProcessResultBuilder::builder(3)
            .completed_count(2)
            .processed_count(3)
            .chunk_count(1)
            .build(),
        Err(BatchProcessResultBuildError::ProcessedCountExceeded {
            completed_count: 2,
            processed_count: 3,
        })
    ));
    assert!(matches!(
        BatchProcessResultBuilder::builder(3)
            .completed_count(2)
            .processed_count(2)
            .build(),
        Err(BatchProcessResultBuildError::MissingChunkForCompletedItems { completed_count: 2 })
    ));
    assert!(matches!(
        BatchProcessResultBuilder::builder(3)
            .completed_count(2)
            .processed_count(2)
            .chunk_count(3)
            .build(),
        Err(BatchProcessResultBuildError::ChunkCountExceeded {
            completed_count: 2,
            chunk_count: 3,
        })
    ));
}
