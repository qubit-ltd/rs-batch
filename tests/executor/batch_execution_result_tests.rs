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
    NoOpProgressReporter,
    ProgressReporter,
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

#[test]
fn test_batch_execution_result_display_summary() {
    let result: BatchExecutionResult<&'static str> =
        BatchExecutionResult::new(3, 2, 1, 1, 0, Duration::from_millis(15), Vec::new());

    let text = result.to_string();

    assert!(text.contains("task_count: 3"));
    assert!(text.contains("completed_count: 2"));
    assert!(text.contains("succeeded_count: 1"));
    assert!(text.contains("failed_count: 1"));
    assert!(text.contains("panicked_count: 0"));
    assert!(text.contains("elapsed: 15ms"));
}

#[test]
fn test_batch_execution_result_into_failures() {
    let failures = vec![BatchTaskFailure::new(4, BatchTaskError::Failed("bad"))];
    let result = BatchExecutionResult::new(5, 1, 0, 1, 0, Duration::from_millis(1), failures);

    let failures = result.into_failures();

    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].index(), 4);
}

#[test]
fn test_batch_task_error_display_and_failure_into_error() {
    let failed = BatchTaskError::Failed("bad");
    let panicked: BatchTaskError<&'static str> = BatchTaskError::Panicked;
    let failure = BatchTaskFailure::new(7, BatchTaskError::Failed("bad"));

    assert_eq!(failed.to_string(), "task failed: bad");
    assert_eq!(panicked.to_string(), "task panicked");
    assert!(failed.is_failed());
    assert!(!failed.is_panicked());
    assert!(!panicked.is_failed());
    assert!(panicked.is_panicked());
    assert!(failure.into_error().is_failed());
}

#[test]
fn test_no_op_progress_reporter_accepts_all_callbacks() {
    let reporter = NoOpProgressReporter;

    reporter.start(3);
    reporter.process(3, 1, 2, Duration::from_millis(5));
    reporter.finish(3, Duration::from_millis(8));
}
