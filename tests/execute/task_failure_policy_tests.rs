// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for `TaskFailurePolicy` through the public sequential executor API.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
};

use qubit_atomic::ArcAtomicCount;
use qubit_batch::{
    BatchTermination,
    SequentialBatchExecutor,
    TaskFailurePolicy,
};

use crate::support::TestTask;

#[test]
fn test_sequential_batch_executor_continues_on_failure_by_default() {
    let successful_tasks = ArcAtomicCount::zero();
    let outcome = SequentialBatchExecutor::new()
        .execute_with_count(
            [
                TestTask::count_success(successful_tasks.clone()),
                TestTask::fail("first failure"),
                TestTask::count_success(successful_tasks.clone()),
            ],
            3,
        )
        .expect("task failures should remain in the outcome");

    assert_eq!(successful_tasks.get(), 2);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 2);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 0);
    assert_eq!(outcome.termination(), BatchTermination::Finished);
}

#[test]
fn test_sequential_batch_executor_continues_when_configured() {
    let successful_tasks = ArcAtomicCount::zero();
    let outcome = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::Continue)
        .build()
        .execute_with_count(
            [
                TestTask::count_success(successful_tasks.clone()),
                TestTask::fail("first failure"),
                TestTask::count_success(successful_tasks.clone()),
            ],
            3,
        )
        .expect("task failures should remain in the outcome");

    assert_eq!(successful_tasks.get(), 2);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 2);
    assert_eq!(outcome.failed_count(), 1);
}

#[test]
fn test_sequential_batch_executor_stops_after_configured_failure_count() {
    let successful_tasks = ArcAtomicCount::zero();
    let outcome = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::StopAfterFailures(
            NonZeroUsize::new(2).expect("two is non-zero"),
        ))
        .build()
        .execute_with_count(
            [
                TestTask::fail("first failure"),
                TestTask::count_success(successful_tasks.clone()),
                TestTask::panic("second failure"),
                TestTask::count_success(successful_tasks.clone()),
            ],
            4,
        )
        .expect("task failures should remain in the outcome");

    assert_eq!(successful_tasks.get(), 1);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(
        outcome.termination(),
        BatchTermination::StoppedByTaskFailurePolicy
    );
}

#[test]
fn test_sequential_batch_executor_does_not_consume_tasks_after_policy_stop() {
    let next_calls = ArcAtomicCount::zero();
    let tasks = CountingTaskIterator::new(
        next_calls.clone(),
        [TestTask::fail("first failure"), TestTask::succeed()],
    );

    let outcome = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build()
        .execute_with_count(tasks, 2)
        .expect("task failures should remain in the outcome");

    assert_eq!(next_calls.get(), 1);
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(
        outcome.termination(),
        BatchTermination::StoppedByTaskFailurePolicy
    );
}

#[test]
fn test_sequential_batch_executor_marks_early_stop_before_count_validation() {
    let outcome = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build()
        .execute_with_count([TestTask::fail("first failure")], 2)
        .expect("early task failure should return its partial outcome");

    assert_eq!(outcome.task_count(), 2);
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(
        outcome.termination(),
        BatchTermination::StoppedByTaskFailurePolicy
    );
}

/// Counts iterator pulls while yielding configured test tasks.
struct CountingTaskIterator {
    /// Counter incremented before each `next` result.
    next_calls: ArcAtomicCount,
    /// Tasks that remain available to the iterator.
    tasks: VecDeque<TestTask>,
}

impl CountingTaskIterator {
    /// Creates an iterator from the supplied tasks.
    ///
    /// # Parameters
    ///
    /// * `next_calls` - Counter incremented on every `next` call.
    /// * `tasks` - Tasks yielded in order.
    ///
    /// # Returns
    ///
    /// An iterator that reports its pulls through `next_calls`.
    fn new<const N: usize>(
        next_calls: ArcAtomicCount,
        tasks: [TestTask; N],
    ) -> Self {
        Self {
            next_calls,
            tasks: VecDeque::from(tasks),
        }
    }
}

impl Iterator for CountingTaskIterator {
    type Item = TestTask;

    /// Returns the next configured task while recording the pull.
    fn next(&mut self) -> Option<Self::Item> {
        self.next_calls.inc();
        self.tasks.pop_front()
    }
}
