// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchExecutionCoordinator`](qubit_batch::ParallelBatchExecutionCoordinator).

use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::Arc,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError,
    ParallelBatchExecutionCoordinator,
};
use qubit_progress::reporter::NoopReporter;

use crate::support::{
    FailingReporter,
    TestTask,
};

#[test]
fn test_parallel_batch_execution_coordinator_records_task_outcomes() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let outcome = coordinator
        .execute(
            [
                TestTask::succeed(),
                TestTask::fail("failed"),
                TestTask::panic("panic"),
            ],
            3,
            |tasks, _count, context| {
                for (index, task) in tasks.into_iter().enumerate() {
                    context.execute_task(index, task).expect(
                        "executed task should be within declared count",
                    );
                }
                3
            },
        )
        .expect("coordinator should return an outcome");

    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
}

#[test]
fn test_parallel_batch_execution_coordinator_reports_count_shortfall() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let error = coordinator
        .execute([TestTask::succeed()], 2, |tasks, _count, context| {
            for (index, task) in tasks.into_iter().enumerate() {
                context
                    .execute_task(index, task)
                    .expect("declared count should include indexed task");
            }
            1
        })
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_execution_coordinator_reports_count_exceeded() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let error = coordinator
        .execute(
            [
                TestTask::succeed(),
                TestTask::succeed(),
                TestTask::succeed(),
            ],
            2,
            |tasks, _count, context| {
                for (index, task) in tasks.into_iter().take(2).enumerate() {
                    context
                        .execute_task(index, task)
                        .expect("declared tasks should execute");
                }
                3
            },
        )
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
            ..
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_execution_coordinator_reports_start_error_as_progress_report()
 {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(FailingReporter::after_successes(0)),
        Duration::ZERO,
    );
    let error = coordinator
        .execute([TestTask::succeed()], 1, |tasks, _count, context| {
            for (index, task) in tasks.into_iter().enumerate() {
                context
                    .execute_task(index, task)
                    .expect("context execution should be attempted");
            }
            1
        })
        .expect_err("start failures should return progress report errors");

    match error {
        BatchExecutionError::ProgressReport { source, .. } => {
            let _ = source;
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_execution_coordinator_propagates_scheduler_panic() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let payload = catch_unwind(AssertUnwindSafe(|| {
        coordinator.execute([1, 2, 3], 3, |tasks, _count, context| {
            for task in tasks {
                context
                    .execute_task(task, TestTask::succeed())
                    .expect("context execution should be attempted");
                if task == 2 {
                    panic!("scheduler failure");
                }
            }
            0
        })
    }))
    .expect_err("scheduler panic should be propagated");

    assert!(payload.is::<&str>());
}
