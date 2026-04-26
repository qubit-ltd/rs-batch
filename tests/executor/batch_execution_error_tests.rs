/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for [`BatchExecutionError`](qubit_batch::BatchExecutionError).

use std::time::Duration;

use qubit_batch::{
    BatchExecutionError,
    BatchExecutionResult,
};

#[test]
fn test_batch_execution_error_shortfall_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        result: BatchExecutionResult::new(3, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert!(error.is_count_shortfall());
    assert!(!error.is_count_exceeded());
    assert_eq!(error.result().completed_count(), 2);

    let result = error.into_result();

    assert_eq!(result.completed_count(), 2);
}

#[test]
fn test_batch_execution_error_exceeded_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        result: BatchExecutionResult::new(2, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert!(error.is_count_exceeded());
    assert!(!error.is_count_shortfall());
    assert_eq!(error.result().task_count(), 2);
    assert_eq!(error.into_result().task_count(), 2);
}
