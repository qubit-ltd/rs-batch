// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for source exhaustion and failure-policy termination.

#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionContext;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::NoopReporter;

#[test]
fn test_next_task_does_not_pull_after_policy_stop() {
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
