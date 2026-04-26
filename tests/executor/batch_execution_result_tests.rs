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

/// Asserts that constructing an invalid batch execution result panics.
fn assert_invalid_result_panics<F>(build: F)
where
    F: FnOnce() + std::panic::UnwindSafe,
{
    assert!(std::panic::catch_unwind(build).is_err());
}

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
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
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
    let failures = vec![BatchTaskFailure::new(1, BatchTaskError::Failed("bad"))];
    let result: BatchExecutionResult<&'static str> =
        BatchExecutionResult::new(3, 2, 1, 1, 0, Duration::from_millis(15), failures);

    let text = result.to_string();

    assert!(text.contains("task_count: 3"));
    assert!(text.contains("completed_count: 2"));
    assert!(text.contains("succeeded_count: 1"));
    assert!(text.contains("failed_count: 1"));
    assert!(text.contains("panicked_count: 0"));
    assert!(text.contains("elapsed: 15ms"));
}

#[test]
fn test_batch_execution_result_display_does_not_require_debug_error() {
    struct NonDebugError;
    let result: BatchExecutionResult<NonDebugError> =
        BatchExecutionResult::new(1, 1, 1, 0, 0, Duration::from_millis(1), Vec::new());

    let text = result.to_string();

    assert!(text.contains("task_count: 1"));
    assert!(text.contains("elapsed: 1ms"));
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
fn test_batch_execution_result_sorts_failure_details() {
    let failures = vec![
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
        BatchTaskFailure::new(1, BatchTaskError::Failed("bad")),
    ];

    let result = BatchExecutionResult::new(3, 3, 1, 1, 1, Duration::from_millis(25), failures);

    assert_eq!(result.failures()[0].index(), 1);
    assert!(result.failures()[0].error().is_failed());
    assert_eq!(result.failures()[1].index(), 2);
    assert!(result.failures()[1].error().is_panicked());
}

#[test]
fn test_batch_execution_result_rejects_completed_count_above_task_count() {
    assert_invalid_result_panics(|| {
        let _: BatchExecutionResult<&'static str> =
            BatchExecutionResult::new(1, 2, 2, 0, 0, Duration::from_millis(1), Vec::new());
    });
}

#[test]
fn test_batch_execution_result_rejects_terminal_count_mismatch() {
    assert_invalid_result_panics(|| {
        let _: BatchExecutionResult<&'static str> =
            BatchExecutionResult::new(2, 2, 1, 0, 0, Duration::from_millis(1), Vec::new());
    });
}

#[test]
fn test_batch_execution_result_rejects_failure_detail_count_mismatch() {
    assert_invalid_result_panics(|| {
        let _: BatchExecutionResult<&'static str> =
            BatchExecutionResult::new(2, 2, 1, 1, 0, Duration::from_millis(1), Vec::new());
    });
}

#[test]
fn test_batch_execution_result_rejects_failed_detail_variant_mismatch() {
    let failures: Vec<BatchTaskFailure<&'static str>> =
        vec![BatchTaskFailure::new(1, BatchTaskError::panicked("panic"))];

    assert_invalid_result_panics(|| {
        BatchExecutionResult::new(2, 2, 1, 1, 0, Duration::from_millis(1), failures);
    });
}

#[test]
fn test_batch_execution_result_rejects_panicked_detail_variant_mismatch() {
    let failures = vec![BatchTaskFailure::new(1, BatchTaskError::Failed("bad"))];

    assert_invalid_result_panics(|| {
        BatchExecutionResult::new(2, 2, 1, 0, 1, Duration::from_millis(1), failures);
    });
}

#[test]
fn test_batch_execution_result_rejects_failure_index_outside_task_range() {
    let failures = vec![BatchTaskFailure::new(2, BatchTaskError::Failed("bad"))];

    assert_invalid_result_panics(|| {
        BatchExecutionResult::new(2, 2, 1, 1, 0, Duration::from_millis(1), failures);
    });
}

#[test]
fn test_batch_execution_result_rejects_failure_count_overflow() {
    assert_invalid_result_panics(|| {
        let _: BatchExecutionResult<&'static str> = BatchExecutionResult::new(
            usize::MAX,
            usize::MAX,
            0,
            usize::MAX,
            1,
            Duration::from_millis(1),
            Vec::new(),
        );
    });
}

#[test]
fn test_batch_execution_result_rejects_terminal_count_overflow() {
    assert_invalid_result_panics(|| {
        let _: BatchExecutionResult<&'static str> = BatchExecutionResult::new(
            usize::MAX,
            usize::MAX,
            usize::MAX,
            1,
            0,
            Duration::from_millis(1),
            Vec::new(),
        );
    });
}

#[test]
fn test_batch_task_error_display_and_failure_into_error() {
    let failed = BatchTaskError::Failed("bad");
    let panicked: BatchTaskError<&'static str> = BatchTaskError::panicked_without_message();
    let panicked_with_message: BatchTaskError<&'static str> = BatchTaskError::panicked("boom");
    let failure = BatchTaskFailure::new(7, BatchTaskError::Failed("bad"));

    assert_eq!(failed.to_string(), "task failed: bad");
    assert_eq!(panicked.to_string(), "task panicked");
    assert_eq!(panicked_with_message.to_string(), "task panicked: boom");
    assert_eq!(failed.panic_message(), None);
    assert_eq!(panicked.panic_message(), None);
    assert_eq!(panicked_with_message.panic_message(), Some("boom"));
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
