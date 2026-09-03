// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for sequential execution outcome accounting.

use qubit_batch::BatchExecutionError;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;

use crate::support::TestTask;

#[test]
fn test_batch_execution_state_tracks_success_failure_and_panic() {
    let executor = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::Continue)
        .build();

    let outcome = executor
        .execute_with_count(
            [TestTask::succeed(), TestTask::fail("failed"), TestTask::panic("panic")],
            3,
        )
        .expect("task failures should stay in the outcome");

    assert_eq!(outcome.task_count(), 3);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(outcome.failure_count(), 2);
    assert_eq!(outcome.failures()[0].index(), 1);
    assert_eq!(outcome.failures()[1].index(), 2);
}

#[test]
fn test_batch_execution_state_counts_report_count_shortfall() {
    let executor = SequentialBatchExecutor::new();

    let error = executor
        .execute_with_count([TestTask::succeed(), TestTask::succeed()], 3)
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
