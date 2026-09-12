// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchExecutionCoordinator`](qubit_batch::ParallelBatchExecutionCoordinator).

use std::convert::Infallible;
use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTermination;
use qubit_batch::ProgressFailure;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionContext;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
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
                Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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
                    let _ = coordinator
                        .execute(
                            [TestTask::succeed()],
                            1,
                            TaskFailurePolicy::Continue,
                            |_tasks, inner_context: &ParallelBatchExecutionContext<&'static str>| {
                                inner_context.execute_task(token);
                                Ok::<(), Infallible>(())
                            },
                        )
                        .expect_err("inner execution should be incomplete after rejecting the token");
                }));
                assert!(inner_result.is_err(), "cross-context token use must panic");
                Ok::<(), Infallible>(())
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
            |_tasks, _context: &ParallelBatchExecutionContext<&'static str>| Ok::<(), Infallible>(()),
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
            Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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
                Ok::<(), Infallible>(())
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

/// Reporter that rejects terminal delivery but accepts start and running
/// events.
struct TerminalFailureReporter;

impl Reporter for TerminalFailureReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        match event.phase() {
            Phase::Succeeded | Phase::Failed => Err(ReporterError::new(io::Error::other("terminal rejected"))),
            _ => Ok(()),
        }
    }
}

/// A secondary terminal failure must not overwrite scheduler rejection.
#[test]
fn test_scheduler_rejection_preserves_terminal_failure() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(TerminalFailureReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                while let Some(token) = context.next_task(&mut tasks) {
                    context.execute_task(token);
                }
                Err(SchedulerError)
            },
        )
        .expect_err("scheduler failure must survive failed terminal delivery");
    assert_eq!(error.scheduler_error(), Some(&SchedulerError));
    assert_eq!(error.outcome().completed_count(), 1);
    assert!(matches!(
        error.progress_report_error(),
        Some(ProgressFailure::Terminal(_))
    ));
}

/// Terminal delivery failure retains the policy termination and task counters.
#[test]
fn test_policy_stop_preserves_termination_on_terminal_failure() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(TerminalFailureReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::fail("failed"), TestTask::succeed()],
            2,
            TaskFailurePolicy::StopOnFirstFailure,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                while let Some(token) = context.next_task(&mut tasks) {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("terminal delivery must fail");
    assert_eq!(
        error.outcome().termination(),
        BatchTermination::StoppedByTaskFailurePolicy
    );
    assert_eq!(error.outcome().completed_count(), 1);
    assert_eq!(error.outcome().failed_count(), 1);
}

/// A completed source keeps counters when terminal success reporting fails.
#[test]
fn test_finished_batch_preserves_counts_on_terminal_failure() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(TerminalFailureReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                while let Some(token) = context.next_task(&mut tasks) {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("terminal success reporting must fail");
    assert_eq!(error.outcome().completed_count(), 1);
    assert_eq!(error.outcome().succeeded_count(), 1);
    assert!(matches!(
        error.progress_report_error(),
        Some(ProgressFailure::Terminal(_))
    ));
}

/// A failed terminal report must retain every task result after Continue drains
/// the source.
#[test]
fn test_finished_failed_batch_preserves_task_failures_on_terminal_failure() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(TerminalFailureReporter), Duration::ZERO);
    let error = coordinator
        .execute(
            [
                TestTask::fail("invalid row"),
                TestTask::succeed(),
                TestTask::panic("task panic"),
            ],
            3,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                while let Some(token) = context.next_task(&mut tasks) {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("failed terminal delivery must retain the completed batch");

    let BatchExecutionError::ProgressReport { source, outcome } = error else {
        panic!("terminal delivery must be the batch-level error");
    };
    let ProgressFailure::Terminal(terminal) = source.as_ref() else {
        panic!("the progress error must identify terminal delivery");
    };
    assert_eq!(outcome.termination(), BatchTermination::Finished);
    assert_eq!(outcome.task_count(), 3);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(outcome.elapsed(), terminal.elapsed());
    assert_eq!(outcome.failures().len(), 2);
    assert_eq!(outcome.failures()[0].index(), 0);
    assert!(matches!(
        outcome.failures()[0].error(),
        BatchTaskError::Failed("invalid row")
    ));
    assert_eq!(outcome.failures()[1].index(), 2);
    assert!(matches!(outcome.failures()[1].error(), BatchTaskError::Panicked { .. }));
}
