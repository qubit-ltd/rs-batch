/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`BatchExecutionResult`](qubit_batch::BatchExecutionResult).

use std::{
    error::Error,
    fmt,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionResult,
    BatchExecutionResultBuildError,
    BatchTaskError,
    BatchTaskFailure,
    NoOpProgressReporter,
    ProgressReporter,
};

/// Builds a valid batch execution result for tests.
///
/// # Parameters
///
/// * `task_count` - Declared task count.
/// * `completed_count` - Completed task count.
/// * `succeeded_count` - Successful task count.
/// * `failed_count` - Failed task count.
/// * `panicked_count` - Panicked task count.
/// * `elapsed` - Monotonic elapsed batch duration.
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
fn test_batch_execution_result_success_state() {
    let result: BatchExecutionResult<&'static str> =
        build_result(3, 3, 3, 0, 0, Duration::from_millis(10), Vec::new());

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
    let result = build_result(3, 3, 1, 1, 1, Duration::from_millis(25), failures);

    assert_eq!(result.failure_count(), 2);
    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 1);
    assert!(result.failures()[0].error().is_failed());
    assert_eq!(result.failures()[1].index(), 2);
    assert!(result.failures()[1].error().is_panicked());
    assert!(!result.is_success());
}

#[test]
fn test_batch_execution_result_clone_and_equality() {
    let failures = vec![
        BatchTaskFailure::new(1, BatchTaskError::Failed("bad")),
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
    ];
    let result = build_result(3, 3, 1, 1, 1, Duration::from_millis(25), failures);

    let cloned = result.clone();

    assert_eq!(cloned, result);
    assert_eq!(cloned.failures()[0].clone(), result.failures()[0]);
    assert_eq!(
        cloned.failures()[1].error().clone(),
        result.failures()[1].error().clone()
    );
}

#[test]
fn test_batch_execution_result_display_summary() {
    let failures = vec![BatchTaskFailure::new(1, BatchTaskError::Failed("bad"))];
    let result: BatchExecutionResult<&'static str> =
        build_result(3, 2, 1, 1, 0, Duration::from_millis(15), failures);

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
        build_result(1, 1, 1, 0, 0, Duration::from_millis(1), Vec::new());

    let text = result.to_string();

    assert!(text.contains("task_count: 1"));
    assert!(text.contains("elapsed: 1ms"));
}

#[test]
fn test_batch_execution_result_into_failures() {
    let failures = vec![BatchTaskFailure::new(4, BatchTaskError::Failed("bad"))];
    let result = build_result(5, 1, 0, 1, 0, Duration::from_millis(1), failures);

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

    let result = build_result(3, 3, 1, 1, 1, Duration::from_millis(25), failures);

    assert_eq!(result.failures()[0].index(), 1);
    assert!(result.failures()[0].error().is_failed());
    assert_eq!(result.failures()[1].index(), 2);
    assert!(result.failures()[1].error().is_panicked());
}

#[test]
fn test_batch_execution_result_try_new_sorts_failure_details() {
    let failures = vec![
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
        BatchTaskFailure::new(1, BatchTaskError::Failed("bad")),
    ];

    let result = BatchExecutionResult::try_new(3, 3, 1, 1, 1, Duration::from_millis(25), failures)
        .expect("valid batch execution result should be created");

    assert_eq!(result.failures()[0].index(), 1);
    assert!(result.failures()[0].error().is_failed());
    assert_eq!(result.failures()[1].index(), 2);
    assert!(result.failures()[1].error().is_panicked());
}

#[test]
fn test_batch_execution_result_try_new_reports_invalid_inputs() {
    assert_eq!(
        BatchExecutionResult::<&'static str>::try_new(
            1,
            2,
            2,
            0,
            0,
            Duration::from_millis(1),
            Vec::new(),
        )
        .expect_err("completed count above task count should be rejected"),
        BatchExecutionResultBuildError::CompletedCountExceeded {
            task_count: 1,
            completed_count: 2,
        }
    );
    assert_eq!(
        BatchExecutionResult::<&'static str>::try_new(
            usize::MAX,
            usize::MAX,
            0,
            usize::MAX,
            1,
            Duration::from_millis(1),
            Vec::new(),
        )
        .expect_err("overflowing failure count should be rejected"),
        BatchExecutionResultBuildError::FailureCountOverflow {
            failed_count: usize::MAX,
            panicked_count: 1,
        }
    );
    assert_eq!(
        BatchExecutionResult::<&'static str>::try_new(
            usize::MAX,
            usize::MAX,
            usize::MAX,
            1,
            0,
            Duration::from_millis(1),
            Vec::new(),
        )
        .expect_err("overflowing terminal count should be rejected"),
        BatchExecutionResultBuildError::TerminalCountOverflow {
            succeeded_count: usize::MAX,
            failure_count: 1,
        }
    );
    assert_eq!(
        BatchExecutionResult::<&'static str>::try_new(
            2,
            2,
            1,
            0,
            0,
            Duration::from_millis(1),
            Vec::new(),
        )
        .expect_err("terminal count mismatch should be rejected"),
        BatchExecutionResultBuildError::TerminalCountMismatch {
            completed_count: 2,
            terminal_count: 1,
            succeeded_count: 1,
            failed_count: 0,
            panicked_count: 0,
        }
    );
    assert_eq!(
        BatchExecutionResult::<&'static str>::try_new(
            2,
            2,
            1,
            1,
            0,
            Duration::from_millis(1),
            Vec::new(),
        )
        .expect_err("failure detail count mismatch should be rejected"),
        BatchExecutionResultBuildError::FailureDetailCountMismatch {
            expected: 1,
            actual: 0,
        }
    );

    let failures = vec![BatchTaskFailure::new(2, BatchTaskError::Failed("bad"))];
    assert_eq!(
        BatchExecutionResult::try_new(2, 2, 1, 1, 0, Duration::from_millis(1), failures)
            .expect_err("out-of-range failure index should be rejected"),
        BatchExecutionResultBuildError::FailureIndexOutOfRange {
            index: 2,
            task_count: 2,
        }
    );

    let failures: Vec<BatchTaskFailure<&'static str>> =
        vec![BatchTaskFailure::new(1, BatchTaskError::panicked("panic"))];
    assert_eq!(
        BatchExecutionResult::try_new(2, 2, 1, 1, 0, Duration::from_millis(1), failures)
            .expect_err("failed detail variant mismatch should be rejected"),
        BatchExecutionResultBuildError::FailureVariantCountMismatch {
            expected_failed: 1,
            actual_failed: 0,
            expected_panicked: 0,
            actual_panicked: 1,
        }
    );

    let error_text = BatchExecutionResultBuildError::FailureVariantCountMismatch {
        expected_failed: 1,
        actual_failed: 0,
        expected_panicked: 0,
        actual_panicked: 1,
    }
    .to_string();
    assert!(error_text.contains("expected_failed 1"));
    assert!(error_text.contains("actual_panicked 1"));
}

#[test]
fn test_batch_execution_result_build_error_clone_and_equality() {
    let errors = [
        BatchExecutionResultBuildError::CompletedCountExceeded {
            task_count: 1,
            completed_count: 2,
        },
        BatchExecutionResultBuildError::FailureCountOverflow {
            failed_count: usize::MAX,
            panicked_count: 1,
        },
        BatchExecutionResultBuildError::TerminalCountOverflow {
            succeeded_count: usize::MAX,
            failure_count: 1,
        },
        BatchExecutionResultBuildError::TerminalCountMismatch {
            completed_count: 2,
            terminal_count: 1,
            succeeded_count: 1,
            failed_count: 0,
            panicked_count: 0,
        },
        BatchExecutionResultBuildError::FailureDetailCountMismatch {
            expected: 1,
            actual: 0,
        },
        BatchExecutionResultBuildError::FailureIndexOutOfRange {
            index: 2,
            task_count: 2,
        },
        BatchExecutionResultBuildError::FailureVariantCountMismatch {
            expected_failed: 1,
            actual_failed: 0,
            expected_panicked: 0,
            actual_panicked: 1,
        },
    ];

    for error in errors {
        assert_eq!(error.clone(), error);
    }
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

#[derive(Debug)]
struct SourceError;

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("source error")
    }
}

impl Error for SourceError {}

#[derive(Debug)]
struct WrappedError {
    source: SourceError,
}

impl fmt::Display for WrappedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wrapped error")
    }
}

impl Error for WrappedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[test]
fn test_batch_task_error_source_preserves_failed_error() {
    let failed = BatchTaskError::Failed(WrappedError {
        source: SourceError,
    });
    let panicked: BatchTaskError<WrappedError> = BatchTaskError::panicked("boom");

    let source = failed
        .source()
        .expect("failed task error should expose wrapped error as source");

    assert_eq!(source.to_string(), "wrapped error");
    assert_eq!(
        source
            .source()
            .expect("wrapped error should preserve its own source")
            .to_string(),
        "source error"
    );
    assert!(panicked.source().is_none());
}

#[test]
fn test_no_op_progress_reporter_accepts_all_callbacks() {
    let reporter = NoOpProgressReporter;

    reporter.start(3);
    reporter.process(3, 1, 2, Duration::from_millis(5));
    reporter.finish(3, Duration::from_millis(8));
}
