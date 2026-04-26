/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for [`ParallelBatchExecutor`](qubit_batch::ParallelBatchExecutor).

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
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
};

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
};

#[test]
fn test_parallel_batch_executor_build_rejects_invalid_parallelism() {
    let error = ParallelBatchExecutor::builder()
        .parallelism(0)
        .build()
        .err()
        .expect("zero parallelism should be rejected");

    assert!(matches!(
        error,
        ParallelBatchExecutorBuildError::ZeroParallelism
    ));
}

#[test]
fn test_parallel_batch_executor_executes_successfully() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_tasks = Arc::clone(&counter);
    let tasks = (0..8).map(move |_| {
        let counter = Arc::clone(&counter_for_tasks);
        move || {
            counter.fetch_add(1, Ordering::AcqRel);
            Ok::<(), &'static str>(())
        }
    });

    let result = executor
        .execute(tasks, 8)
        .expect("parallel batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 8);
    assert_eq!(result.completed_count(), 8);
    assert_eq!(result.succeeded_count(), 8);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_parallel_batch_executor_collects_failures_and_panics() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = (0..3).map(|index| {
        move || match index {
            0 => Ok::<(), &'static str>(()),
            1 => Err("failed"),
            _ => panic!("panic in parallel batch"),
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
}

#[test]
fn test_parallel_batch_executor_reports_count_shortfall() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
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
fn test_parallel_batch_executor_reports_count_exceeded() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
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
fn test_parallel_batch_executor_runs_tasks_concurrently() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let active_for_tasks = Arc::clone(&active);
    let max_active_for_tasks = Arc::clone(&max_active);
    let tasks = (0..8).map(move |_| {
        let active = Arc::clone(&active_for_tasks);
        let max_active = Arc::clone(&max_active_for_tasks);
        move || {
            let current = active.fetch_add(1, Ordering::AcqRel) + 1;
            update_max(&max_active, current);
            thread::sleep(Duration::from_millis(30));
            active.fetch_sub(1, Ordering::AcqRel);
            Ok::<(), &'static str>(())
        }
    });

    let result = executor
        .execute(tasks, 8)
        .expect("parallel batch should succeed");

    assert_eq!(result.completed_count(), 8);
    assert!(max_active.load(Ordering::Acquire) > 1);
}

#[test]
fn test_parallel_batch_executor_falls_back_to_sequential_below_threshold() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(10)
        .build()
        .expect("parallel executor should build");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let active_for_tasks = Arc::clone(&active);
    let max_active_for_tasks = Arc::clone(&max_active);
    let tasks = (0..4).map(move |_| {
        let active = Arc::clone(&active_for_tasks);
        let max_active = Arc::clone(&max_active_for_tasks);
        move || {
            let current = active.fetch_add(1, Ordering::AcqRel) + 1;
            update_max(&max_active, current);
            thread::sleep(Duration::from_millis(10));
            active.fetch_sub(1, Ordering::AcqRel);
            Ok::<(), &'static str>(())
        }
    });

    let result = executor.execute(tasks, 4).expect("batch should succeed");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(max_active.load(Ordering::Acquire), 1);
}

#[test]
fn test_parallel_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = ParallelBatchExecutor::builder()
        .parallelism(2)
        .parallel_threshold(1)
        .report_interval(Duration::from_millis(10))
        .reporter_arc(reporter.clone())
        .build()
        .expect("parallel executor should build");
    let tasks = (0..4).map(|_| {
        || {
            thread::sleep(Duration::from_millis(60));
            Ok::<(), &'static str>(())
        }
    });

    let result = executor.execute(tasks, 4).expect("batch should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 4);
    assert!(matches!(
        events.first(),
        Some(ProgressEvent::Start { total_count: 4 })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 4,
            active_count,
            ..
        } if *active_count > 0
    )));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 4, .. })
    ));
}

fn update_max(max_active: &AtomicUsize, current: usize) {
    let mut observed = max_active.load(Ordering::Acquire);
    while current > observed {
        match max_active.compare_exchange(observed, current, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(value) => observed = value,
        }
    }
}
