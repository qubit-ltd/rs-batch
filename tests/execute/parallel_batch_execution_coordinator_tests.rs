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
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionContext;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::reporter::NoopReporter;
use thiserror::Error;

use crate::support::FailingReporter;
use crate::support::TestTask;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("scheduler rejected work")]
struct SchedulerError;

#[test]
fn test_parallel_batch_execution_coordinator_records_task_outcomes() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let outcome = coordinator
        .execute(
            [TestTask::succeed(), TestTask::fail("failed"), TestTask::panic("panic")],
            3,
            TaskFailurePolicy::Continue,
            |tasks, context: &ParallelBatchExecutionContext<&'static str>| {
                for task in tasks {
                    let task = context.accept_task(task).expect("task should be accepted");
                    context.execute_task(task);
                }
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect("coordinator should return an outcome");

    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
}

#[test]
fn test_parallel_batch_execution_coordinator_reports_count_shortfall() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            2,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    let task = context.accept_task(task).expect("task should be accepted");
                    context.execute_task(task);
                }
                Ok::<(), std::convert::Infallible>(())
            },
        )
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
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed(), TestTask::succeed(), TestTask::succeed()],
            2,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    if let Some(task) = context.accept_task(task) {
                        context.execute_task(task);
                    }
                }
                Ok::<(), std::convert::Infallible>(())
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
fn test_parallel_batch_execution_coordinator_count_exceeded_precedes_failure_stop() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::fail("failed"), TestTask::succeed()],
            1,
            TaskFailurePolicy::StopOnFirstFailure,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                let first = context
                    .accept_task(tasks.next().expect("first task should exist"))
                    .expect("first task should be accepted");
                let second = context.accept_task(tasks.next().expect("second task should exist"));
                assert!(second.is_none(), "the overflow task must be rejected");
                context.execute_task(first);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect_err("count overflow must not be masked by the failure policy");

    assert!(matches!(error, BatchExecutionError::CountExceeded { .. }));
}

#[test]
fn test_parallel_batch_execution_context_rejects_token_from_another_execution() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let outer_error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, outer_context: &ParallelBatchExecutionContext<&'static str>| {
                let token = outer_context
                    .accept_task(tasks.into_iter().next().expect("outer task should exist"))
                    .expect("outer task should be accepted");
                let inner_result = catch_unwind(AssertUnwindSafe(|| {
                    coordinator
                        .execute(
                            [TestTask::succeed()],
                            1,
                            TaskFailurePolicy::Continue,
                            |_tasks, inner_context: &ParallelBatchExecutionContext<&'static str>| {
                                inner_context.execute_task(token);
                                Ok::<(), std::convert::Infallible>(())
                            },
                        )
                        .expect_err("inner execution should be incomplete after rejecting the token");
                }));
                assert!(inner_result.is_err(), "cross-context token use must panic");
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect_err("outer execution intentionally leaves its token incomplete");

    assert!(matches!(outer_error, BatchExecutionError::IncompleteSchedule { .. }));
}

#[test]
fn test_parallel_batch_execution_coordinator_reports_start_error_as_progress_report() {
    let coordinator =
        ParallelBatchExecutionCoordinator::new(Arc::new(FailingReporter::after_successes(0)), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |_tasks, _context: &ParallelBatchExecutionContext<&'static str>| Ok::<(), std::convert::Infallible>(()),
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
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let payload = catch_unwind(AssertUnwindSafe(|| {
        coordinator.execute([1, 2, 3], 3, TaskFailurePolicy::Continue, |tasks, context| {
            for task in tasks {
                let task_token = context
                    .accept_task(TestTask::succeed())
                    .expect("task should be accepted");
                context.execute_task(task_token);
                if task == 2 {
                    panic!("scheduler failure");
                }
            }
            Ok::<(), std::convert::Infallible>(())
        })
    }))
    .expect_err("scheduler panic should be propagated");

    assert!(payload.is::<&str>());
}

#[test]
fn test_parallel_batch_execution_coordinator_uses_context_observed_count() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            2,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    let task = context.accept_task(task).expect("task should be accepted");
                    context.execute_task(task);
                }
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect_err("context observations should determine count validation");

    assert!(error.is_count_shortfall());
}

#[test]
fn test_parallel_batch_execution_coordinator_rejects_dropped_accepted_tasks() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context: &ParallelBatchExecutionContext<&'static str>| {
                for task in tasks {
                    let _dropped = context.accept_task(task).expect("task should be accepted");
                }
                Ok::<(), std::convert::Infallible>(())
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

#[test]
fn test_parallel_batch_execution_coordinator_prioritizes_incomplete_schedule_over_shortfall() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            10,
            TaskFailurePolicy::Continue,
            |tasks, context: &ParallelBatchExecutionContext<&'static str>| {
                if let Some(task) = tasks.into_iter().next() {
                    let _ = context.accept_task(task).expect("task should be accepted");
                }
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect_err("accepted but dropped work must be reported");

    match error {
        BatchExecutionError::IncompleteSchedule {
            expected,
            observed,
            accepted,
            completed,
            ..
        } => {
            assert_eq!(expected, 10);
            assert_eq!(observed, 1);
            assert_eq!(accepted, 1);
            assert_eq!(completed, 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_execution_coordinator_returns_scheduler_error_directly() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    let token = context.accept_task(task).expect("task should be accepted");
                    context.execute_task(token);
                }
                Err(SchedulerError)
            },
        )
        .expect_err("scheduler failure should be preserved");

    assert!(error.is_schedule_failed());
    assert_eq!(error.scheduler_error(), Some(&SchedulerError));
    assert_eq!(error.outcome().completed_count(), 1);
}
