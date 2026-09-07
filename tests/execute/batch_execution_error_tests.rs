// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`BatchExecutionError`](qubit_batch::BatchExecutionError).

use std::cell::Cell;
use std::io;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchOutcome;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::ProgressFailure;
use qubit_batch::SequentialBatchExecutor;

use crate::support::FailingReporter;

/// Builds a valid batch outcome for error helper tests.
///
/// # Parameters
///
/// * `task_count` - Declared task count.
/// * `completed_count` - Completed task count.
///
/// # Returns
///
/// A valid batch outcome.
fn build_outcome<E>(task_count: usize, completed_count: usize) -> BatchOutcome<E> {
    BatchOutcomeBuilder::builder(task_count)
        .completed_count(completed_count)
        .succeeded_count(completed_count)
        .build()
        .expect("test outcome should satisfy batch outcome invariants")
}

#[test]
fn test_batch_execution_error_shortfall_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        outcome: build_outcome(3, 2),
        report_error: None,
    };

    assert!(error.is_count_shortfall());
    assert!(!error.is_count_exceeded());
    assert_eq!(error.outcome().completed_count(), 2);

    match &error {
        BatchExecutionError::CountShortfall { expected, actual, .. } => {
            assert_eq!(*expected, 3);
            assert_eq!(*actual, 2);
        }
        _ => panic!("expect count shortfall"),
    }

    let outcome = error.into_outcome();
    assert_eq!(outcome.completed_count(), 2);
}

#[test]
fn test_batch_execution_error_exceeded_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        outcome: build_outcome(2, 2),
        report_error: None,
    };

    assert!(error.is_count_exceeded());
    assert!(!error.is_count_shortfall());
    assert_eq!(error.outcome().task_count(), 2);
    match &error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            ..
        } => {
            assert_eq!(*expected, 2);
            assert_eq!(*observed_at_least, 3);
        }
        _ => panic!("expect count exceeded"),
    }

    let outcome = error.into_outcome();
    assert_eq!(outcome.task_count(), 2);
    assert_eq!(outcome.completed_count(), 2);
}

/// Builds a real start failure without exposing any progress implementation.
fn start_failure() -> Box<ProgressFailure> {
    let error = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(0))
        .build()
        .execute_with_count([|| Ok::<(), &'static str>(())], 1)
        .expect_err("reporter must reject start");
    match error {
        BatchExecutionError::ProgressReport { source, .. } => source,
        other => panic!("unexpected progress error: {other:?}"),
    }
}

/// Scheduler conversion preserves every non-scheduler variant and partial data.
#[test]
fn test_scheduler_error_mapping_preserves_all_variants_and_secondary_errors() {
    let errors: Vec<BatchExecutionError<&'static str, io::Error>> = vec![
        BatchExecutionError::ProgressReport {
            source: start_failure(),
            outcome: build_outcome(2, 1),
        },
        BatchExecutionError::ScheduleFailed {
            source: io::Error::other("rejected"),
            outcome: build_outcome(2, 1),
            report_error: Some(start_failure()),
        },
        BatchExecutionError::CountShortfall {
            expected: 2,
            actual: 1,
            outcome: build_outcome(2, 1),
            report_error: Some(start_failure()),
        },
        BatchExecutionError::CountExceeded {
            expected: 2,
            observed_at_least: 3,
            outcome: build_outcome(2, 1),
            report_error: Some(start_failure()),
        },
        BatchExecutionError::IncompleteSchedule {
            expected: 2,
            accepted: 2,
            observed: 2,
            completed: 1,
            outcome: build_outcome(2, 1),
            report_error: Some(start_failure()),
        },
    ];
    for (index, error) in errors.into_iter().enumerate() {
        let calls = Cell::new(0);
        let mapped = error.map_scheduler_error(|source| {
            calls.set(calls.get() + 1);
            io::Error::other(format!("mapped: {source}"))
        });
        assert_eq!(calls.get(), usize::from(index == 1));
        assert_eq!(mapped.is_schedule_failed(), index == 1);
        assert_eq!(mapped.is_count_shortfall(), index == 2);
        assert_eq!(mapped.is_count_exceeded(), index == 3);
        assert_eq!(mapped.is_incomplete_schedule(), index == 4);
        assert_eq!(
            mapped.scheduler_error().map(ToString::to_string),
            (index == 1).then(|| "mapped: rejected".to_owned())
        );
        assert!(mapped.progress_report_error().is_some());
        match &mapped {
            BatchExecutionError::CountShortfall { expected, actual, .. } => assert_eq!((*expected, *actual), (2, 1)),
            BatchExecutionError::CountExceeded {
                expected,
                observed_at_least,
                ..
            } => assert_eq!((*expected, *observed_at_least), (2, 3)),
            BatchExecutionError::IncompleteSchedule {
                expected,
                accepted,
                observed,
                completed,
                ..
            } => {
                assert_eq!((*expected, *accepted, *observed, *completed), (2, 2, 2, 1));
            }
            BatchExecutionError::ProgressReport { .. } | BatchExecutionError::ScheduleFailed { .. } => {}
            other => panic!("unexpected mapped error: {other:?}"),
        }
        let outcome = mapped.into_outcome();
        assert_eq!((outcome.task_count(), outcome.completed_count()), (2, 1));
    }
}

#[test]
fn test_batch_execution_error_accessors() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(1)
        .succeeded_count(1)
        .build()
        .expect("outcome should be valid");
    let shortfall: BatchExecutionError<_, std::convert::Infallible> = BatchExecutionError::CountShortfall {
        expected: 2,
        actual: 1,
        outcome: outcome.clone(),
        report_error: None,
    };
    assert!(shortfall.is_count_shortfall());
    assert!(!shortfall.is_count_exceeded());
    assert_eq!(shortfall.outcome().completed_count(), 1);
    assert_eq!(
        shortfall.to_string(),
        "batch task count shortfall: expected 2, actual 1"
    );
    assert_eq!(shortfall.into_outcome(), outcome.clone());

    let exceeded: BatchExecutionError<_, std::convert::Infallible> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        outcome,
        report_error: None,
    };
    assert!(!exceeded.is_count_shortfall());
    assert!(exceeded.is_count_exceeded());
    assert_eq!(exceeded.outcome().completed_count(), 1);
    assert_eq!(
        exceeded.to_string(),
        "batch task count exceeded: expected 2, observed at least 3"
    );
    assert_eq!(exceeded.into_outcome().completed_count(), 1);
}
