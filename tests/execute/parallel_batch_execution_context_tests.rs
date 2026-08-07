// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for [`ParallelBatchExecutionContext`].

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError,
    ParallelBatchExecutionCoordinator,
    ProgressFailure,
};
use qubit_progress::Reporter;

use crate::support::{
    FailingReporter,
    ProgressEvent,
    RecordingReporter,
    TestTask,
};

#[test]
fn test_parallel_batch_execution_context_execute_task_notifies_running_progress()
 {
    let reporter = Arc::new(RecordingReporter::new());
    let reporter_for_coordinator: Arc<dyn Reporter> = reporter.clone();
    let coordinator = ParallelBatchExecutionCoordinator::new(
        reporter_for_coordinator,
        Duration::ZERO,
    );
    let outcome = coordinator
        .execute(
            [
                TestTask::sleep_success(Duration::from_millis(2)),
                TestTask::sleep_success(Duration::from_millis(2)),
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
        .expect("coordinator should complete");

    assert_eq!(outcome.completed_count(), 2);
    let events = reporter.events();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ProgressEvent::Process { .. })),
        "tasks should trigger at least one running-progress event",
    );
    assert!(
        matches!(events.last(), Some(ProgressEvent::Finish { .. })),
        "execution should end with a finish event",
    );
}

#[test]
fn test_parallel_batch_execution_context_rejects_tasks_after_declared_count() {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(RecordingReporter::new()),
        Duration::ZERO,
    );
    let error = coordinator
        .execute(
            [TestTask::succeed(), TestTask::succeed()],
            1,
            |tasks, context| {
                for task in tasks {
                    if let Some(task) = context.accept_task(task) {
                        context.execute_task(task);
                    }
                }
            },
        )
        .expect_err(
            "the second observed task should exceed the declared count",
        );

    assert!(error.is_count_exceeded());
}

#[test]
fn test_parallel_batch_execution_context_auto_reporter_failure_is_reported_as_progress_error()
 {
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(FailingReporter::after_successes(1)),
        Duration::ZERO,
    );

    let error =
        coordinator.execute([TestTask::succeed()], 1, |tasks, context| {
            for task in tasks {
                if let Some(task) = context.accept_task(task) {
                    context.execute_task(task);
                }
                for _ in 0..100 {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        });

    let error = error
        .expect_err("auto reporter failure should be mapped to progress error");
    let BatchExecutionError::ProgressReport { source, .. } = error else {
        panic!("auto reporter failure should map to progress report error");
    };
    let ProgressFailure::AutoReporter(_) = source.as_ref() else {
        panic!("auto reporter failure should be preserved");
    };
}
