/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Integration tests for [`BatchExecutor::call`](qubit_batch::BatchExecutor::call)
//! and the internal callable runnable wrapper.

use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};

use crate::support::TestCallable;

#[test]
fn test_sequential_batch_executor_calls_callables_and_collects_values() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![
        TestCallable::returning(10),
        TestCallable::returning(20),
        TestCallable::returning(30),
    ];

    let result = executor.call(tasks, 3).expect("call batch should succeed");

    assert_eq!(result.outcome().completed_count(), 3);
    assert_eq!(result.values(), &[Some(10), Some(20), Some(30)]);
    assert_eq!(result.into_values(), vec![Some(10), Some(20), Some(30)]);

    let tasks = vec![TestCallable::returning(40)];
    let result = executor.call(tasks, 1).expect("call batch should succeed");
    assert_eq!(result.into_outcome().completed_count(), 1);

    let tasks = vec![TestCallable::returning(50)];
    let result = executor.call(tasks, 1).expect("call batch should succeed");
    let (outcome, values) = result.into_parts();
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(values, vec![Some(50)]);
}

#[test]
fn test_sequential_batch_executor_call_preserves_failure_indexes() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![
        TestCallable::returning(10),
        TestCallable::fail("failed"),
        TestCallable::panic("panic in callable"),
        TestCallable::returning(40),
    ];

    let result = executor
        .call(tasks, 4)
        .expect("callable failures should stay in the batch result");

    assert_eq!(result.values(), &[Some(10), None, None, Some(40)]);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    assert_eq!(result.outcome().failures()[0].index(), 1);
    assert_eq!(result.outcome().failures()[1].index(), 2);
}
