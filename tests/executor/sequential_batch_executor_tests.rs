/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for [`SequentialBatchExecutor`](qubit_batch::SequentialBatchExecutor).

use std::{
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError,
    BatchExecutor,
    SequentialBatchExecutor,
};

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
    TestTask,
};

#[test]
fn test_sequential_batch_executor_executes_successfully() {
    let executor = SequentialBatchExecutor::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let tasks = vec![
        TestTask::count_success(Arc::clone(&counter)),
        TestTask::count_success(Arc::clone(&counter)),
        TestTask::count_success(Arc::clone(&counter)),
    ];

    let result = executor
        .execute(tasks, 3)
        .expect("sequential batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_sequential_batch_executor_accessors_and_value_reporter() {
    let executor = SequentialBatchExecutor::new()
        .with_reporter(RecordingProgressReporter::new())
        .with_report_interval(Duration::from_millis(25));

    assert_eq!(executor.report_interval(), Duration::from_millis(25));
    executor.reporter().start(1);
}

#[test]
fn test_sequential_batch_executor_collects_failures_and_panics() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panic in sequential batch"),
    ];

    let result = executor
        .execute(tasks, 3)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 1);
    assert_eq!(result.failed_count(), 1);
    assert_eq!(result.panicked_count(), 1);
    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 1);
    assert_eq!(result.failures()[1].index(), 2);
    assert_eq!(
        result.failures()[1].error().panic_message(),
        Some("panic in sequential batch")
    );
}

#[test]
fn test_sequential_batch_executor_records_non_string_panic_without_message() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![TestTask::panic_usize(7)];

    let result = executor
        .execute(tasks, 1)
        .expect("task panic should stay in the batch result");

    assert_eq!(result.completed_count(), 1);
    assert_eq!(result.panicked_count(), 1);
    assert_eq!(result.failures()[0].error().panic_message(), None);
}

#[test]
fn test_sequential_batch_executor_reports_count_shortfall() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute(tasks, 3)
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(result.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_sequential_batch_executor_reports_count_exceeded() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute(tasks, 1)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 1);
            assert_eq!(observed_at_least, 2);
            assert_eq!(result.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_sequential_batch_executor_for_each_maps_items() {
    let executor = SequentialBatchExecutor::new();

    let result = executor
        .for_each(0..4, 4, |value| {
            if value == 2 {
                Err("bad item")
            } else {
                Ok::<(), &'static str>(())
            }
        })
        .expect("for_each should keep item failures in the result");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failed_count(), 1);
    assert_eq!(result.failures()[0].index(), 2);
}

#[test]
fn test_sequential_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = SequentialBatchExecutor::new()
        .with_reporter_arc(reporter.clone())
        .with_report_interval(Duration::from_millis(10));
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
    ];

    let result = executor
        .execute(tasks, 3)
        .expect("sequential batch should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 3);
    assert!(matches!(
        events.first(),
        Some(ProgressEvent::Start { total_count: 3 })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 3,
            active_count: 0,
            completed_count,
            ..
        } if *completed_count >= 1
    )));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 3, .. })
    ));
}
