// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for [`ParallelBatchExecutionContext`].

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use qubit_batch::{ParallelBatchExecutionCoordinator, ProgressFailure};

use crate::support::{FailingReporter, ProgressEvent, RecordingReporter, TestTask};

#[test]
fn test_parallel_batch_execution_context_execute_task_notifies_running_progress() {
    let reporter = Arc::new(RecordingReporter::new());
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::clone(&reporter),
        Duration::ZERO,
    );
    let outcome = coordinator
        .execute(
            [
                TestTask::sleep_success(Duration::from_millis(2)),
                TestTask::sleep_success(Duration::from_millis(2)),
            ],
            2,
            |tasks, _count, context| {
                for (index, task) in tasks.into_iter().enumerate() {
                    context
                        .execute_task(index, task)
                        .expect("executed task should be within declared count");
                }
                2
            },
        )
        .expect("coordinator should complete");

    assert_eq!(outcome.completed_count(), 2);
    let events = reporter.events();
    assert!(
        events.iter().any(|event| matches!(event, ProgressEvent::Process { .. })),
        "tasks should trigger at least one running-progress event",
    );
    assert!(
        matches!(events.last(), Some(ProgressEvent::Finish { .. })),
        "execution should end with a finish event",
    );
}

#[test]
fn test_parallel_batch_execution_context_exposes_task_index_errors() {
    let seen_out_of_range = Arc::new(AtomicBool::new(false));
    let coordinator =
        ParallelBatchExecutionCoordinator::new(Arc::new(RecordingReporter::new()), Duration::ZERO);
    let seen = Arc::clone(&seen_out_of_range);

    let _ = coordinator
        .execute([TestTask::succeed()], 1, |tasks, _count, context| {
            let mut observed = 0usize;
            for (index, task) in tasks.into_iter().enumerate() {
                context
                    .execute_task(index, task)
                    .expect("index should be in range");
                observed = index + 1;
                if context.execute_task(index + 1, TestTask::succeed()).is_err() {
                    seen.store(true, Ordering::Relaxed);
                }
            }
            observed
        })
        .expect("scheduler should still report progress with invalid index");

    assert!(seen.load(Ordering::Relaxed), "out-of-range task index should be reported");
}

#[test]
fn test_parallel_batch_execution_context_auto_reporter_failure_is_reported_as_progress_error() {
    let seen_failure = Arc::new(AtomicBool::new(false));
    let failure_seen = Arc::clone(&seen_failure);
    let coordinator = ParallelBatchExecutionCoordinator::new(
        Arc::new(FailingReporter::after_successes(1)),
        Duration::ZERO,
    );

    let error = coordinator.execute([TestTask::succeed()], 1, |tasks, _count, context| {
        for (index, task) in tasks {
            context
                .execute_task(index, task)
                .expect("task index 0 should be in range");
            failure_seen.store(context.reporting_failed(), Ordering::Relaxed);
        }
        1
    });

    let error = error.expect_err("auto reporter failure should be mapped to progress error");
    let BatchExecutionError::ProgressReport { source, .. } = error else {
        panic!("auto reporter failure should map to progress report error");
    };
    let ProgressFailure::AutoReporter(_) = source.as_ref() else {
        panic!("auto reporter failure should be preserved");
    };
    assert!(seen_failure.load(Ordering::Relaxed));
}
