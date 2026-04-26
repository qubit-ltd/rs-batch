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
    TestTask,
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
fn test_parallel_batch_executor_build_rejects_zero_report_interval() {
    let error = ParallelBatchExecutor::builder()
        .report_interval(Duration::ZERO)
        .build()
        .err()
        .expect("zero report interval should be rejected");

    assert!(matches!(
        error,
        ParallelBatchExecutorBuildError::ZeroReportInterval
    ));
}

#[test]
fn test_parallel_batch_executor_build_rejects_zero_stack_size() {
    let error = ParallelBatchExecutor::builder()
        .stack_size(0)
        .build()
        .err()
        .expect("zero stack size should be rejected");

    assert!(matches!(
        error,
        ParallelBatchExecutorBuildError::ZeroStackSize
    ));
}

#[test]
fn test_parallel_batch_executor_new_default_and_accessors() {
    let executor = ParallelBatchExecutor::new(2).expect("parallel executor should build");
    let default_executor = ParallelBatchExecutor::default();

    assert_eq!(executor.parallelism(), 2);
    assert_eq!(
        executor.parallel_threshold(),
        ParallelBatchExecutor::DEFAULT_PARALLEL_THRESHOLD
    );
    assert_eq!(
        executor.report_interval(),
        ParallelBatchExecutor::DEFAULT_REPORT_INTERVAL
    );
    executor.reporter().start(0);
    assert!(default_executor.parallelism() >= 1);
}

#[test]
fn test_parallel_batch_executor_builder_custom_options() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(2)
        .parallel_threshold(3)
        .report_interval(Duration::from_millis(25))
        .reporter(RecordingProgressReporter::new())
        .no_reporter()
        .thread_name_prefix("qubit-batch-test")
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("parallel executor should build with custom options");

    assert_eq!(executor.parallelism(), 2);
    assert_eq!(executor.parallel_threshold(), 3);
    assert_eq!(executor.report_interval(), Duration::from_millis(25));
}

#[test]
fn test_parallel_batch_executor_executes_successfully() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let counter = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| TestTask::count_success(Arc::clone(&counter)))
        .collect::<Vec<_>>();

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
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panic in parallel batch"),
    ];

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
fn test_parallel_batch_executor_orders_failures_by_task_index() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(2)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::fail_after_sleep("slow failure", Duration::from_millis(50)),
        TestTask::fail("fast failure"),
    ];

    let result = executor
        .execute(tasks, 2)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 0);
    assert_eq!(result.failures()[1].index(), 1);
}

#[test]
fn test_parallel_batch_executor_reports_count_shortfall() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
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
fn test_parallel_batch_executor_handles_huge_declared_count_without_preallocation() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(2)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![TestTask::succeed()];

    let error = executor
        .execute(tasks, usize::MAX)
        .expect_err("shortfall should be reported without preallocating count");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, usize::MAX);
            assert_eq!(actual, 1);
            assert_eq!(result.completed_count(), 1);
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
fn test_parallel_batch_executor_reports_count_exceeded_in_parallel_path() {
    let executor = ParallelBatchExecutor::builder()
        .parallelism(4)
        .parallel_threshold(1)
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::succeed(),
        TestTask::succeed(),
    ];

    let error = executor
        .execute(tasks, 2)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(result.completed_count(), 2);
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
    let tasks = (0..8)
        .map(|_| {
            TestTask::track_concurrency(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(30),
            )
        })
        .collect::<Vec<_>>();

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
    let tasks = (0..4)
        .map(|_| {
            TestTask::track_concurrency(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(10),
            )
        })
        .collect::<Vec<_>>();

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
    let tasks = (0..4)
        .map(|_| TestTask::sleep_success(Duration::from_millis(60)))
        .collect::<Vec<_>>();

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
    assert!(events.iter().all(|event| match event {
        ProgressEvent::Process { active_count, .. } => *active_count <= 2,
        _ => true,
    }));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 4, .. })
    ));
}
