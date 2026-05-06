/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests covering indexed task behavior through the public parallel executor.

use qubit_batch::{
    BatchExecutor,
    ParallelBatchExecutor,
};

use crate::support::TestTask;

#[test]
fn test_parallel_batch_executor_records_original_failure_indexes() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("first failure"),
        TestTask::succeed(),
        TestTask::fail("second failure"),
    ];

    let outcome = executor
        .execute(tasks, 4)
        .expect("task failures should stay in the batch outcome");
    let mut failure_indexes = outcome
        .failures()
        .iter()
        .map(|failure| failure.index())
        .collect::<Vec<_>>();
    failure_indexes.sort_unstable();

    assert_eq!(outcome.completed_count(), 4);
    assert_eq!(outcome.succeeded_count(), 2);
    assert_eq!(outcome.failed_count(), 2);
    assert_eq!(failure_indexes, vec![1, 3]);
}
