// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for execution state accounting.

use std::time::Duration;

use qubit_batch::{
    BatchExecutionState,
    BatchExecutionStateError,
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
    TaskFailurePolicy,
};
use qubit_progress::{
    Metric,
    NoopReporter,
    Progress,
};

use crate::support::TestTask;

#[test]
fn test_batch_execution_state_counts_success_failure_and_panic() {
    let executor = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::Continue)
        .build();
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panic in execution state"),
    ];

    let outcome = executor
        .execute_with_count(tasks, 3)
        .expect("task-level failures should stay in the outcome");

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
fn test_batch_execution_state_executes_indexed_tasks_safely() {
    let reporter = NoopReporter;
    let progress = Progress::builder(&reporter)
        .metric(Metric::new("tasks", "Tasks").total(2))
        .start()
        .expect("progress must start");
    let metric = progress
        .metric("tasks")
        .expect("configured metric must exist");
    let state = BatchExecutionState::<&'static str>::new(2, metric);

    state
        .execute_task(0, TestTask::fail("failed"))
        .expect("in-range task should be recorded");
    state
        .execute_task(1, TestTask::panic("panicked"))
        .expect("in-range task should be recorded");

    assert_eq!(
        state.execute_task(2, TestTask::succeed()),
        Err(BatchExecutionStateError::TaskIndexOutOfRange {
            index: 2,
            task_count: 2,
        }),
    );

    let outcome = state
        .try_into_outcome(Duration::ZERO)
        .expect("state should build a consistent outcome");
    assert_eq!(outcome.completed_count(), 2);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
}

#[test]
#[allow(deprecated)]
fn test_batch_execution_state_public_api_builds_outcome() {
    let reporter = NoopReporter;
    let progress = Progress::builder(&reporter)
        .metric(Metric::new("tasks", "Tasks").total(2))
        .start()
        .expect("progress must start");
    let metric = progress
        .metric("tasks")
        .expect("configured metric must exist");
    let state = BatchExecutionState::<&'static str>::new(2, metric);

    assert_eq!(state.record_task_observed(), 1);
    state
        .record_task_started()
        .expect("metric start must succeed");
    state
        .record_task_succeeded()
        .expect("metric success must succeed");
    assert_eq!(state.record_task_observed(), 2);
    state
        .record_task_started()
        .expect("metric start must succeed");
    state
        .record_task_panicked(1, BatchTaskError::panicked("boom"))
        .expect("metric failure must succeed");

    let outcome = state.into_outcome(Duration::from_millis(7));
    assert_eq!(outcome.task_count(), 2);
    assert_eq!(outcome.completed_count(), 2);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(outcome.elapsed(), Duration::from_millis(7));
    assert_eq!(outcome.failures()[0].index(), 1);
    assert_eq!(outcome.failures()[0].error().panic_message(), Some("boom"));
}
