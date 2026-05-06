/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests covering parallel result state through public batch outcomes.

use qubit_batch::BatchExecutor;
use qubit_batch::ParallelBatchExecutor;

use crate::support::TestTask;

#[test]
fn test_parallel_batch_executor_result_state_counts_terminal_outcomes() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panicked"),
    ];

    let outcome = executor
        .execute(tasks, 3)
        .expect("task failures should stay in the batch outcome");
    let mut failure_indexes = outcome
        .failures()
        .iter()
        .map(|failure| failure.index())
        .collect::<Vec<_>>();
    failure_indexes.sort_unstable();

    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(failure_indexes, vec![1, 2]);
}
