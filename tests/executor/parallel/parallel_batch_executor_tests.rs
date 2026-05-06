/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ParallelBatchExecutor`](qubit_batch::ParallelBatchExecutor).

use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError, BatchExecutor, ParallelBatchExecutor, ParallelBatchExecutorBuildError,
};
use qubit_function::Runnable;

use crate::support::{
    PanickingProgressReporter, ProgressEvent, ProgressPanicPhase, RecordingProgressReporter,
    TestTask, panic_payload_message,
};

#[test]
fn test_parallel_batch_executor_builds_default_and_custom_config() {
    let default_executor = ParallelBatchExecutor::default();
    assert_eq!(
        default_executor.thread_count(),
        ParallelBatchExecutor::default_thread_count()
    );
    assert_eq!(
        default_executor.sequential_threshold(),
        ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    let new_executor = ParallelBatchExecutor::new(2).expect("executor should build");
    assert_eq!(new_executor.thread_count(), 2);

    let executor = ParallelBatchExecutor::builder()
        .thread_count(3)
        .sequential_threshold(2)
        .report_interval(Duration::from_millis(25))
        .build()
        .expect("custom executor should build");

    assert_eq!(executor.thread_count(), 3);
    assert_eq!(executor.sequential_threshold(), 2);
    assert_eq!(executor.report_interval(), Duration::from_millis(25));
    assert!(Arc::strong_count(executor.reporter()) >= 1);
}

#[test]
fn test_parallel_batch_executor_rejects_invalid_builder_config() {
    assert!(matches!(
        ParallelBatchExecutor::builder().thread_count(0).build(),
        Err(ParallelBatchExecutorBuildError::ZeroThreadCount)
    ));
    assert!(matches!(
        ParallelBatchExecutor::builder()
            .report_interval(Duration::ZERO)
            .build(),
        Err(ParallelBatchExecutorBuildError::ZeroReportInterval)
    ));
}

#[test]
fn test_parallel_batch_executor_executes_with_configured_parallelism() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("parallel executor should build");
    let active_count = Arc::new(AtomicUsize::new(0));
    let max_active_count = Arc::new(AtomicUsize::new(0));
    let tasks = (0..6)
        .map(|_| {
            ActiveTrackingTask::new(
                Arc::clone(&active_count),
                Arc::clone(&max_active_count),
                Duration::from_millis(20),
            )
        })
        .collect::<Vec<_>>();

    let result = executor
        .execute(tasks, 6)
        .expect("parallel batch should succeed");

    assert_eq!(result.completed_count(), 6);
    assert_eq!(result.succeeded_count(), 6);
    assert_eq!(result.failure_count(), 0);
    assert!(max_active_count.load(Ordering::Acquire) > 1);
    assert!(max_active_count.load(Ordering::Acquire) <= 2);
}

#[test]
fn test_parallel_batch_executor_uses_sequential_threshold() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(8)
        .build()
        .expect("parallel executor should build");
    let active_count = Arc::new(AtomicUsize::new(0));
    let max_active_count = Arc::new(AtomicUsize::new(0));
    let tasks = (0..4)
        .map(|_| {
            ActiveTrackingTask::new(
                Arc::clone(&active_count),
                Arc::clone(&max_active_count),
                Duration::from_millis(1),
            )
        })
        .collect::<Vec<_>>();

    let result = executor
        .execute(tasks, 4)
        .expect("threshold fallback should succeed");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(max_active_count.load(Ordering::Acquire), 1);
}

#[test]
fn test_parallel_batch_executor_supports_non_static_tasks() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("parallel executor should build");
    let first = AtomicUsize::new(0);
    let second = AtomicUsize::new(0);
    let tasks = vec![
        BorrowingTask { counter: &first },
        BorrowingTask { counter: &second },
    ];

    let result = executor
        .execute(tasks, 2)
        .expect("borrowed tasks should execute");

    assert_eq!(result.succeeded_count(), 2);
    assert_eq!(first.load(Ordering::Acquire), 1);
    assert_eq!(second.load(Ordering::Acquire), 1);
}

#[test]
fn test_parallel_batch_executor_collects_failures_and_panics() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
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
    assert_eq!(result.failures()[0].index(), 1);
    assert_eq!(result.failures()[1].index(), 2);
    assert_eq!(
        result.failures()[1].error().panic_message(),
        Some("panic in parallel batch")
    );
}

#[test]
fn test_parallel_batch_executor_reports_count_shortfall() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
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
            outcome,
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_executor_reports_count_exceeded() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
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
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::from_millis(10))
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
    ];

    let result = executor
        .execute(tasks, 3)
        .expect("parallel batch should succeed");
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
            active_count,
            completed_count,
        } if *active_count > 0 || *completed_count > 0
    )));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 3, .. })
    ));
}

#[test]
fn test_parallel_batch_executor_propagates_progress_reporter_finish_panic() {
    const PANIC_MESSAGE: &str = "parallel progress finish panic";
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Finish,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("parallel executor should build");
    let tasks = vec![TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute(tasks, 1)))
        .expect_err("progress reporter finish panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_parallel_batch_executor_propagates_progress_reporter_process_panic() {
    const PANIC_MESSAGE: &str = "parallel progress process panic";
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(1))
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Process,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("parallel executor should build");
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(50)),
        TestTask::sleep_success(Duration::from_millis(50)),
    ];

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute(tasks, 2)))
        .expect_err("progress reporter process panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

/// Task that records the maximum number of concurrently active tasks.
#[derive(Debug)]
struct ActiveTrackingTask {
    /// Shared count of currently running tasks.
    active_count: Arc<AtomicUsize>,
    /// Shared maximum active count observed by any task.
    max_active_count: Arc<AtomicUsize>,
    /// Time to keep the task active.
    duration: Duration,
}

impl ActiveTrackingTask {
    /// Creates an active-tracking task.
    ///
    /// # Parameters
    ///
    /// * `active_count` - Shared active counter.
    /// * `max_active_count` - Shared maximum active counter.
    /// * `duration` - Time to keep the task active.
    ///
    /// # Returns
    ///
    /// A task configured with the supplied counters.
    fn new(
        active_count: Arc<AtomicUsize>,
        max_active_count: Arc<AtomicUsize>,
        duration: Duration,
    ) -> Self {
        Self {
            active_count,
            max_active_count,
            duration,
        }
    }
}

impl Runnable<&'static str> for ActiveTrackingTask {
    /// Runs this task while updating active counters.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    fn run(&mut self) -> Result<(), &'static str> {
        let active = self.active_count.fetch_add(1, Ordering::AcqRel) + 1;
        update_max(&self.max_active_count, active);
        thread::sleep(self.duration);
        self.active_count.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }
}

/// Updates `max_value` if `candidate` is larger.
///
/// # Parameters
///
/// * `max_value` - Atomic maximum to update.
/// * `candidate` - Candidate maximum value.
fn update_max(max_value: &AtomicUsize, candidate: usize) {
    let mut current = max_value.load(Ordering::Acquire);
    while candidate > current {
        match max_value.compare_exchange(current, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

/// Task that borrows a counter from the caller's stack.
struct BorrowingTask<'a> {
    /// Borrowed counter incremented by this task.
    counter: &'a AtomicUsize,
}

impl fmt::Debug for BorrowingTask<'_> {
    /// Formats this task for failed test output.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BorrowingTask").finish()
    }
}

impl Runnable<&'static str> for BorrowingTask<'_> {
    /// Increments the borrowed counter.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    fn run(&mut self) -> Result<(), &'static str> {
        self.counter.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
