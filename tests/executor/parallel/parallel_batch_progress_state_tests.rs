/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests covering parallel progress state through public progress events.

use std::sync::Arc;
use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    ParallelBatchExecutor,
};

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
    TestTask,
};

#[test]
fn test_parallel_batch_executor_running_progress_reports_shared_counts() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::from_millis(1))
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
    ];

    let outcome = executor
        .execute(tasks, 4)
        .expect("parallel batch should succeed");
    let events = reporter.events();
    let running_events = events
        .iter()
        .filter_map(|event| match event {
            ProgressEvent::Process {
                active_count,
                completed_count,
                ..
            } => Some((*active_count, *completed_count)),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(outcome.completed_count(), 4);
    assert!(
        running_events
            .iter()
            .any(|(active_count, _)| *active_count > 0 && *active_count <= 2)
    );
    assert!(
        running_events
            .iter()
            .all(|(_, completed_count)| *completed_count <= 4)
    );
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish {
            total_count: 4,
            completed_count: 4,
        })
    ));
}
