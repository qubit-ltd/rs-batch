/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for batch outcomes and execution state.

use std::{
    error::Error,
    fmt,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError,
    BatchExecutionState,
    BatchOutcome,
    BatchOutcomeBuildError,
    BatchTaskError,
    BatchTaskFailure,
};

#[test]
fn test_batch_outcome_records_all_failures() {
    let failures = vec![
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
        BatchTaskFailure::new(1, BatchTaskError::Failed("failed")),
    ];

    let outcome = BatchOutcome::try_new(3, 3, 1, 1, 1, Duration::from_millis(5), failures)
        .expect("outcome should be valid");

    assert_eq!(outcome.task_count(), 3);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(outcome.failure_count(), 2);
    assert!(!outcome.is_success());
    assert_eq!(outcome.failures()[0].index(), 1);
    assert_eq!(outcome.failures()[1].index(), 2);
    assert!(outcome.to_string().contains("BatchOutcome"));
}

#[test]
fn test_batch_outcome_rejects_invalid_counters() {
    let error = BatchOutcome::<&'static str>::try_new(2, 3, 3, 0, 0, Duration::ZERO, Vec::new())
        .expect_err("completed count should be invalid");

    assert_eq!(
        error,
        BatchOutcomeBuildError::CompletedCountExceeded {
            task_count: 2,
            completed_count: 3,
        }
    );
}

#[test]
fn test_batch_outcome_rejects_failure_detail_mismatches() {
    let failure = BatchTaskFailure::new(3, BatchTaskError::Failed("failed"));
    assert!(matches!(
        BatchOutcome::try_new(2, 1, 0, 1, 0, Duration::ZERO, vec![failure]),
        Err(BatchOutcomeBuildError::FailureIndexOutOfRange { .. })
    ));

    let failure: BatchTaskFailure<&'static str> =
        BatchTaskFailure::new(0, BatchTaskError::panicked("panic"));
    assert!(matches!(
        BatchOutcome::try_new(2, 1, 0, 1, 0, Duration::ZERO, vec![failure]),
        Err(BatchOutcomeBuildError::FailureVariantCountMismatch { .. })
    ));

    assert!(matches!(
        BatchOutcome::<&'static str>::try_new(2, 1, 0, usize::MAX, 1, Duration::ZERO, Vec::new()),
        Err(BatchOutcomeBuildError::FailureCountOverflow { .. })
    ));

    assert!(matches!(
        BatchOutcome::<&'static str>::try_new(
            usize::MAX,
            0,
            usize::MAX,
            1,
            0,
            Duration::ZERO,
            Vec::new(),
        ),
        Err(BatchOutcomeBuildError::TerminalCountOverflow { .. })
    ));

    assert!(matches!(
        BatchOutcome::<&'static str>::try_new(2, 1, 1, 1, 0, Duration::ZERO, Vec::new()),
        Err(BatchOutcomeBuildError::TerminalCountMismatch { .. })
    ));

    assert!(matches!(
        BatchOutcome::<&'static str>::try_new(2, 1, 0, 1, 0, Duration::ZERO, Vec::new()),
        Err(BatchOutcomeBuildError::FailureDetailCountMismatch { .. })
    ));
}

#[test]
fn test_batch_outcome_into_failures_and_success_state() {
    let outcome = BatchOutcome::<&'static str>::try_new(1, 1, 1, 0, 0, Duration::ZERO, Vec::new())
        .expect("success outcome should be valid");
    assert!(outcome.is_success());
    assert!(outcome.into_failures().is_empty());
}

#[test]
fn test_batch_execution_state_builds_progress_counters_and_outcome() {
    let mut state = BatchExecutionState::new(2);
    state.record_task_started();
    state.record_task_succeeded();
    state.record_task_started();
    state.record_task_failed(1, "failed");

    let counters = state.progress_counters();
    assert_eq!(counters.total_count(), Some(2));
    assert_eq!(counters.active_count(), 0);
    assert_eq!(counters.completed_count(), 2);
    assert_eq!(counters.succeeded_count(), 1);
    assert_eq!(counters.failed_count(), 1);

    let outcome = state.into_outcome(Duration::from_millis(12));
    assert_eq!(outcome.failure_count(), 1);
    assert_eq!(outcome.failures()[0].index(), 1);
}

#[test]
fn test_batch_execution_state_folds_panics_into_progress_failures() {
    let mut state = BatchExecutionState::<&'static str>::new(1);
    assert_eq!(state.task_count(), 1);
    assert_eq!(state.completed_count(), 0);

    state.record_task_started();
    state.record_task_panicked(0, BatchTaskError::panicked("boom"));

    let counters = state.progress_counters();
    assert_eq!(counters.completed_count(), 1);
    assert_eq!(counters.failed_count(), 1);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[test]
fn test_batch_task_error_helpers_display_and_source() {
    let failed = BatchTaskError::Failed(TestError("failed"));
    assert!(failed.is_failed());
    assert!(!failed.is_panicked());
    assert_eq!(failed.to_string(), "task failed: failed");
    assert_eq!(failed.source().expect("source").to_string(), "failed");

    let panicked = BatchTaskError::<TestError>::panicked("panic");
    assert!(!panicked.is_failed());
    assert!(panicked.is_panicked());
    assert_eq!(panicked.panic_message(), Some("panic"));
    assert_eq!(panicked.to_string(), "task panicked: panic");
    assert!(panicked.source().is_none());

    let panicked_without_message = BatchTaskError::<TestError>::panicked_without_message();
    assert_eq!(panicked_without_message.panic_message(), None);
    assert_eq!(panicked_without_message.to_string(), "task panicked");
}

#[test]
fn test_batch_task_failure_into_error() {
    let failure = BatchTaskFailure::new(4, BatchTaskError::Failed("failed"));
    assert_eq!(failure.index(), 4);
    assert_eq!(failure.into_error(), BatchTaskError::Failed("failed"));
}

#[test]
fn test_batch_execution_error_accessors() {
    let outcome = BatchOutcome::<&'static str>::try_new(2, 1, 1, 0, 0, Duration::ZERO, Vec::new())
        .expect("outcome should be valid");
    let shortfall = BatchExecutionError::CountShortfall {
        expected: 2,
        actual: 1,
        outcome: outcome.clone(),
    };
    assert!(shortfall.is_count_shortfall());
    assert!(!shortfall.is_count_exceeded());
    assert_eq!(shortfall.outcome().completed_count(), 1);
    assert_eq!(
        shortfall.to_string(),
        "batch task count shortfall: expected 2, actual 1"
    );
    assert_eq!(shortfall.clone().into_outcome(), outcome);

    let exceeded = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        outcome,
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
