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
    thread,
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
};

#[test]
fn test_sequential_batch_executor_executes_successfully() {
    let executor = SequentialBatchExecutor::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_tasks = Arc::clone(&counter);
    let tasks = (0..3).map(move |_| {
        let counter = Arc::clone(&counter_for_tasks);
        move || {
            counter.fetch_add(1, Ordering::AcqRel);
            Ok::<(), &'static str>(())
        }
    });

    let result = executor
        .execute(tasks, 3)
        .expect("sequential batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_sequential_batch_executor_collects_failures_and_panics() {
    let executor = SequentialBatchExecutor::new();
    let tasks = (0..3).map(|index| {
        move || match index {
            0 => Ok::<(), &'static str>(()),
            1 => Err("failed"),
            _ => panic!("panic in sequential batch"),
        }
    });

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
}

#[test]
fn test_sequential_batch_executor_reports_count_shortfall() {
    let executor = SequentialBatchExecutor::new();
    let tasks = (0..2).map(|_| || Ok::<(), &'static str>(()));

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
    let tasks = (0..2).map(|_| || Ok::<(), &'static str>(()));

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
    let tasks = (0..3).map(|_| {
        || {
            thread::sleep(Duration::from_millis(20));
            Ok::<(), &'static str>(())
        }
    });

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
