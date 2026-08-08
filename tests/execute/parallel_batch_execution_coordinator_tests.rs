// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchExecutionCoordinator`](qubit_batch::ParallelBatchExecutionCoordinator).

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::ParallelBatchExecutionCoordinator;
use qubit_progress::reporter::NoopReporter;

use crate::support::FailingReporter;
use crate::support::TestTask;

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
            |tasks, context| {
                for task in tasks {
                    let task = context
                        .accept_task(task)
                        .expect("task should be accepted");
                    context.execute_task(task);
                }
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
        .execute([TestTask::succeed()], 2, |tasks, context| {
            for task in tasks {
                let task =
                    context.accept_task(task).expect("task should be accepted");
                context.execute_task(task);
            }
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
            |tasks, context| {
                for task in tasks {
                    if let Some(task) = context.accept_task(task) {
                        context.execute_task(task);
                    }
                }
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
        .execute(
            [TestTask::succeed()],
            1,
            |_tasks,
             _context: &qubit_batch::ParallelBatchExecutionContext<
                &'static str,
            >| {},
        )
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
        coordinator.execute([1, 2, 3], 3, |tasks, context| {
            for task in tasks {
                let task_token = context
                    .accept_task(TestTask::succeed())
                    .expect("task should be accepted");
                context.execute_task(task_token);
                if task == 2 {
                    panic!("scheduler failure");
                }
            }
        })
    }))
    .expect_err("scheduler panic should be propagated");

    assert!(payload.is::<&str>());
}

#[test]
fn test_parallel_batch_execution_coordinator_uses_context_observed_count() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let error = coordinator
        .execute([TestTask::succeed()], 2, |tasks, context| {
            for task in tasks {
                let task =
                    context.accept_task(task).expect("task should be accepted");
                context.execute_task(task);
            }
        })
        .expect_err("context observations should determine count validation");

    assert!(error.is_count_shortfall());
}

#[test]
fn test_parallel_batch_execution_coordinator_rejects_dropped_accepted_tasks() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(NoopReporter),
        Duration::ZERO,
    );
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            |tasks,
             context: &qubit_batch::ParallelBatchExecutionContext<
                &'static str,
            >| {
                for task in tasks {
                    let _dropped = context
                        .accept_task(task)
                        .expect("task should be accepted");
                }
            },
        )
        .expect_err("accepted tasks must be executed before returning");

    match error {
        BatchExecutionError::IncompleteSchedule {
            expected,
            accepted,
            completed,
            outcome,
            ..
        } => {
            assert_eq!(expected, 1);
            assert_eq!(accepted, 1);
            assert_eq!(completed, 0);
            assert_eq!(outcome.completed_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
