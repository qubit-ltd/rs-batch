/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Behavioral coverage for the internal batch execution state.

use qubit_batch::{BatchExecutor, SequentialBatchExecutor};

use crate::support::TestTask;

#[test]
fn test_batch_execution_state_counts_success_failure_and_panic() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panic in execution state"),
    ];

    let outcome = executor
        .execute(tasks, 3)
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
