/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`BatchProcessResult`](qubit_batch::BatchProcessResult).

use std::time::Duration;

use qubit_batch::BatchProcessResult;

#[test]
fn test_batch_process_result_accessors_success_and_display() {
    let result = BatchProcessResult::builder(3)
        .completed_count(3)
        .processed_count(3)
        .chunk_count(2)
        .elapsed(Duration::from_millis(25))
        .build()
        .expect("process result counters should be valid");

    assert_eq!(result.item_count(), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.processed_count(), 3);
    assert_eq!(result.chunk_count(), 2);
    assert_eq!(result.elapsed(), Duration::from_millis(25));
    assert!(result.is_success());
    assert_eq!(result.to_string(), "processed 3/3 items in 2 chunks (25ms)");
}

#[test]
fn test_batch_process_result_reports_incomplete_success_state() {
    let incomplete = BatchProcessResult::builder(3)
        .completed_count(2)
        .processed_count(2)
        .chunk_count(1)
        .build()
        .expect("partial process result counters should be valid");
    let unprocessed = BatchProcessResult::builder(3)
        .completed_count(3)
        .processed_count(2)
        .chunk_count(2)
        .build()
        .expect("under-processed result counters should be valid");

    assert!(!incomplete.is_success());
    assert!(!unprocessed.is_success());
}
