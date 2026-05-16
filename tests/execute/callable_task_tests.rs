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

use std::panic::{
    AssertUnwindSafe,
    catch_unwind,
};

use qubit_function::Runnable;

use qubit_batch::{
    BatchExecutionError,
    BatchExecutor,
    BatchOutcome,
    BatchOutcomeBuilder,
    ParallelBatchExecutor,
    SequentialBatchExecutor,
};

use crate::support::{
    TestCallable,
    panic_payload_message,
};

struct OverconsumingExecutor;

impl BatchExecutor for OverconsumingExecutor {
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        for mut task in tasks.into_iter().take(count.saturating_add(1)) {
            let _ = task.run();
        }
        let outcome = BatchOutcomeBuilder::builder(count)
            .completed_count(count)
            .succeeded_count(count)
            .build()
            .expect("synthetic outcome counters should be valid");
        Ok(outcome)
    }
}

#[test]
fn test_sequential_batch_executor_calls_callables_and_collects_values() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![
        TestCallable::returning(10),
        TestCallable::returning(20),
        TestCallable::returning(30),
    ];

    let result = executor
        .call_with_count(tasks, 3)
        .expect("call batch should succeed");

    assert_eq!(result.outcome().completed_count(), 3);
    assert_eq!(result.values(), &[Some(10), Some(20), Some(30)]);
    assert_eq!(result.into_values(), vec![Some(10), Some(20), Some(30)]);

    let tasks = vec![TestCallable::returning(40)];
    let result = executor
        .call_with_count(tasks, 1)
        .expect("call batch should succeed");
    assert_eq!(result.into_outcome().completed_count(), 1);

    let tasks = vec![TestCallable::returning(50)];
    let result = executor
        .call_with_count(tasks, 1)
        .expect("call batch should succeed");
    let (outcome, values) = result.into_parts();
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(values, vec![Some(50)]);
}

#[test]
fn test_batch_executor_call_derives_count_from_exact_iterator() {
    let executor = SequentialBatchExecutor::new();

    let result = executor
        .call([TestCallable::returning(10), TestCallable::returning(20)])
        .expect("array length should be exact");

    assert_eq!(result.outcome().completed_count(), 2);
    assert_eq!(result.values(), &[Some(10), Some(20)]);
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
        .call_with_count(tasks, 4)
        .expect("callable failures should stay in the batch result");

    assert_eq!(result.values(), &[Some(10), None, None, Some(40)]);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    assert_eq!(result.outcome().failures()[0].index(), 1);
    assert_eq!(result.outcome().failures()[1].index(), 2);
}

#[test]
fn test_sequential_batch_executor_call_collects_many_values_by_index() {
    const COUNT: usize = 2048;

    let executor = SequentialBatchExecutor::new();
    let tasks = (0..COUNT)
        .map(|index| TestCallable::returning(index as i32))
        .collect::<Vec<_>>();

    let result = executor
        .call_with_count(tasks, COUNT)
        .expect("large callable batch should succeed");

    assert_eq!(result.outcome().completed_count(), COUNT);
    assert_eq!(result.values().len(), COUNT);
    for (index, value) in result.values().iter().enumerate() {
        assert_eq!(*value, Some(index as i32));
    }
}

#[test]
fn test_parallel_batch_executor_call_collects_values_by_index() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestCallable::returning(10),
        TestCallable::fail("failed"),
        TestCallable::panic("panic in callable"),
        TestCallable::returning(40),
    ];

    let result = executor
        .call_with_count(tasks, 4)
        .expect("callable failures should stay in the batch result");

    assert_eq!(result.values(), &[Some(10), None, None, Some(40)]);
    assert_eq!(result.outcome().completed_count(), 4);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    assert_eq!(result.outcome().failures()[0].index(), 1);
    assert_eq!(result.outcome().failures()[1].index(), 2);
}

#[test]
fn test_parallel_batch_executor_call_reports_count_mismatches() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("parallel executor should build");

    let shortfall = executor
        .call_with_count(vec![TestCallable::returning(10)], 2)
        .expect_err("call shortfall should be reported");
    match shortfall {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let exceeded = executor
        .call_with_count(
            vec![
                TestCallable::returning(10),
                TestCallable::returning(20),
                TestCallable::returning(30),
            ],
            2,
        )
        .expect_err("call overflow should be reported");
    match exceeded {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_batch_executor_call_panics_when_callable_wrapper_reports_out_of_range_index() {
    let executor = OverconsumingExecutor;

    let payload = catch_unwind(AssertUnwindSafe(|| {
        let _ = executor.call_with_count(
            vec![TestCallable::returning(10), TestCallable::returning(20)],
            1,
        );
    }))
    .expect_err("out-of-range callable output should panic");

    assert_eq!(
        panic_payload_message(payload.as_ref()),
        Some("callable index must be within the declared count")
    );
}
