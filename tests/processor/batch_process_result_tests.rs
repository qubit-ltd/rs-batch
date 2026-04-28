/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for [`BatchProcessResult`](qubit_batch::BatchProcessResult).

use std::time::Duration;

use qubit_batch::BatchProcessResult;

#[test]
fn test_batch_process_result_accessors_success_and_display() {
    let result = BatchProcessResult::new(3, 3, 3, 2, Duration::from_millis(25));

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
    let incomplete = BatchProcessResult::new(3, 2, 2, 1, Duration::ZERO);
    let unprocessed = BatchProcessResult::new(3, 3, 2, 2, Duration::ZERO);

    assert!(!incomplete.is_success());
    assert!(!unprocessed.is_success());
}
