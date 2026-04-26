/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for [`BatchExecutionResult`](qubit_batch::BatchExecutionResult).

use std::time::Duration;

use qubit_batch::{
    BatchExecutionResult,
    BatchTaskError,
    BatchTaskFailure,
};

#[test]
fn test_batch_execution_result_success_state() {
    let result: BatchExecutionResult<&'static str> =
        BatchExecutionResult::new(3, 3, 3, 0, 0, Duration::from_millis(10), Vec::new());

    assert_eq!(result.task_count(), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failed_count(), 0);
    assert_eq!(result.panicked_count(), 0);
    assert_eq!(result.failure_count(), 0);
    assert!(result.is_success());
}

#[test]
fn test_batch_execution_result_failure_details() {
    let failures = vec![
        BatchTaskFailure::new(1, BatchTaskError::Failed("bad")),
        BatchTaskFailure::new(2, BatchTaskError::Panicked),
    ];
    let result = BatchExecutionResult::new(3, 3, 1, 1, 1, Duration::from_millis(25), failures);

    assert_eq!(result.failure_count(), 2);
    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 1);
    assert!(result.failures()[0].error().is_failed());
    assert_eq!(result.failures()[1].index(), 2);
    assert!(result.failures()[1].error().is_panicked());
    assert!(!result.is_success());
}
