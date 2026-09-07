// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for source exhaustion and failure-policy termination.

#![allow(clippy::result_large_err)]

use std::sync::mpsc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchOutcome;
use qubit_batch::BatchTermination;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::TaskFailurePolicy;

fn run_exhausted(expected: usize) -> Result<BatchOutcome<&'static str>, BatchExecutionError<&'static str>> {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build()
        .expect("parallel executor should build");
    let (sender, receiver) = mpsc::channel();
    let mut sender = Some(sender);
    let mut task = Some(move || {
        receiver
            .recv_timeout(Duration::from_secs(3))
            .expect("source should be observed as exhausted before the task completes");
        Err::<(), _>("failed after source exhaustion")
    });
    let source = std::iter::from_fn(move || {
        if task.is_some() {
            task.take()
        } else {
            if let Some(sender) = sender.take() {
                sender.send(()).expect("task should still be waiting");
            }
            None
        }
    });
    executor.execute_with_count(source, expected)
}

#[test]
fn test_exhausted_short_source_is_not_policy_stop() {
    match run_exhausted(2).expect_err("short source should report a count error") {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!((expected, actual), (2, 1));
            assert_eq!(outcome.completed_count(), 1);
            assert_eq!(outcome.failed_count(), 1);
            assert_eq!(outcome.failures()[0].index(), 0);
        }
        error => panic!("unexpected error: {error:?}"),
    }
}

#[test]
fn test_exhausted_exact_source_finishes_despite_failure_threshold() {
    let outcome = run_exhausted(1).expect("exact source should return its outcome");
    assert_eq!(outcome.termination(), BatchTermination::Finished);
    assert_eq!(outcome.failed_count(), 1);
    assert!(!outcome.is_success());
}

#[test]
fn test_next_task_does_not_pull_after_policy_stop() {
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::sync::Arc;

    use qubit_batch::execute::spi::ParallelBatchExecutionContext;
    use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
    use qubit_progress::NoopReporter;

    let pulls = Cell::new(0usize);
    let source = std::iter::from_fn(|| {
        pulls.set(pulls.get() + 1);
        Some(|| Err::<(), _>("failure"))
    });
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let outcome = coordinator
        .execute(
            source,
            100,
            TaskFailurePolicy::StopOnFirstFailure,
            |tasks, context: &ParallelBatchExecutionContext<&'static str>| {
                let mut tasks = tasks;
                let token = context.next_task(&mut tasks).expect("first task should be accepted");
                context.execute_task(token);
                assert!(context.next_task(&mut tasks).is_none());
                Ok::<(), Infallible>(())
            },
        )
        .expect("policy stop should return a partial outcome");
    assert_eq!(pulls.get(), 1);
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
}
