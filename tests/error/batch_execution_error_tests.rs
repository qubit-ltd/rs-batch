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
    BatchTaskFailure,
};

/// Builds a valid batch execution result for error helper tests.
///
/// # Parameters
///
/// * `task_count` - Declared task count.
/// * `completed_count` - Completed task count.
/// * `succeeded_count` - Successful task count.
/// * `failed_count` - Failed task count.
/// * `panicked_count` - Panicked task count.
/// * `elapsed` - Elapsed batch duration.
/// * `failures` - Detailed task failures.
///
/// # Returns
///
/// A valid batch execution result.
fn build_result<E>(
    task_count: usize,
    completed_count: usize,
    succeeded_count: usize,
    failed_count: usize,
    panicked_count: usize,
    elapsed: Duration,
    failures: Vec<BatchTaskFailure<E>>,
) -> BatchExecutionResult<E> {
    BatchExecutionResult::try_new(
        task_count,
        completed_count,
        succeeded_count,
        failed_count,
        panicked_count,
        elapsed,
        failures,
    )
    .expect("test result should satisfy batch execution result invariants")
}

#[test]
fn test_batch_execution_error_shortfall_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        result: build_result(3, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert!(error.is_count_shortfall());
    assert!(!error.is_count_exceeded());
    assert_eq!(error.result().completed_count(), 2);

    let result = error.into_result();

    assert_eq!(result.completed_count(), 2);
}

#[test]
fn test_batch_execution_error_shortfall_clone_and_equality() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        result: build_result(3, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert_eq!(error.clone(), error);
}

#[test]
fn test_batch_execution_error_exceeded_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        result: build_result(2, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert!(error.is_count_exceeded());
    assert!(!error.is_count_shortfall());
    assert_eq!(error.result().task_count(), 2);
    assert_eq!(error.into_result().task_count(), 2);
}

#[test]
fn test_batch_execution_error_exceeded_clone_and_equality() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        result: build_result(2, 2, 2, 0, 0, Duration::from_millis(10), Vec::new()),
    };

    assert_eq!(error.clone(), error);
}
