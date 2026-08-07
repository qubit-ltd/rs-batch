// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`SequentialBatchExecutor`](qubit_batch::SequentialBatchExecutor).

use std::{
    cell::RefCell,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use qubit_atomic::ArcAtomicCount;
use qubit_batch::{
    BatchExecutionError,
    SequentialBatchExecutor,
    TaskFailurePolicy,
};
use qubit_function::Callable;
use qubit_function::Runnable;

use crate::support::{
    FailingReporter,
    PanickingReporter,
    ProgressEvent,
    ProgressPanicPhase,
    RecordingReporter,
    TestTask,
    panic_payload_message,
};

#[test]
fn test_sequential_batch_executor_returns_progress_report_error() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(1))
        .build();

    let error = executor
        .execute_with_count([TestTask::succeed()], 1)
        .expect_err("failing reporter should fail batch execution");

    assert!(matches!(&error, BatchExecutionError::ProgressReport { .. }));
    assert_eq!(error.outcome().completed_count(), 1);
}

#[test]
fn test_sequential_batch_executor_preserves_count_error_when_failure_report_fails()
 {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(1))
        .build();

    let error = executor
        .execute_with_count([TestTask::succeed()], 2)
        .expect_err("shortfall should remain the primary error");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            report_error,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
            assert_eq!(outcome.completed_count(), 1);
            assert!(report_error.is_some());
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_sequential_batch_executor_executes_successfully() {
    let executor = SequentialBatchExecutor::new();
    let counter = ArcAtomicCount::zero();
    let tasks = vec![
        TestTask::count_success(counter.clone()),
        TestTask::count_success(counter.clone()),
        TestTask::count_success(counter.clone()),
    ];

    let result = executor
        .execute_with_count(tasks, 3)
        .expect("sequential batch should succeed");

    assert_eq!(counter.get(), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_sequential_batch_executor_runs_non_send_local_tasks() {
    let executor = SequentialBatchExecutor::new();
    let counter = Rc::new(RefCell::new(0));
    let tasks = (0..3)
        .map(|_| LocalTask(Rc::clone(&counter)))
        .collect::<Vec<_>>();

    let result = executor
        .execute_with_count(tasks, 3)
        .expect("local tasks should execute sequentially");

    assert!(result.is_success());
    assert_eq!(*counter.borrow(), 3);
}

#[test]
fn test_sequential_batch_executor_runs_non_send_for_each_action() {
    let executor = SequentialBatchExecutor::new();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let action_seen = Rc::clone(&seen);

    let result = executor
        .for_each_with_count([1, 2, 3], 3, move |item| {
            action_seen.borrow_mut().push(item);
            Ok::<(), &'static str>(())
        })
        .expect("local action should execute sequentially");

    assert!(result.is_success());
    assert_eq!(&*seen.borrow(), &[1, 2, 3]);
}

#[test]
fn test_sequential_batch_executor_calls_non_send_local_callables() {
    let executor = SequentialBatchExecutor::new();
    let prefix = Rc::new(String::from("local"));
    let callables = vec![
        LocalCallable(Rc::clone(&prefix)),
        LocalCallable(Rc::clone(&prefix)),
    ];

    let result = executor
        .call_with_count(callables, 2)
        .expect("local callables should execute sequentially");

    assert_eq!(
        result.into_values(),
        vec![
            Some(Rc::new(String::from("local"))),
            Some(Rc::new(String::from("local")))
        ]
    );
}

#[test]
fn test_sequential_batch_executor_accepts_non_debug_errors() {
    let executor = SequentialBatchExecutor::new();
    let result = executor.execute_with_count([NonDebugTask], 1);

    match result {
        Ok(outcome) => assert!(outcome.is_success()),
        Err(_) => panic!("non-debug task error type should be accepted"),
    }
}

#[test]
fn test_sequential_batch_executor_accessors_and_value_reporter() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(RecordingReporter::new())
        .report_interval(Duration::from_millis(25))
        .build();
    let no_reporter_executor =
        SequentialBatchExecutor::builder().no_reporter().build();

    assert_eq!(executor.report_interval(), Duration::from_millis(25));
    assert!(Arc::strong_count(executor.reporter()) >= 1);
    assert!(Arc::strong_count(no_reporter_executor.reporter()) >= 1);
    assert_eq!(executor.task_failure_policy(), TaskFailurePolicy::Continue);
}

#[test]
fn test_sequential_batch_executor_exact_execute_accepts_non_send_tasks() {
    let executor = SequentialBatchExecutor::new();
    let counter = Rc::new(RefCell::new(0));
    let result = executor
        .execute([LocalTask(Rc::clone(&counter))])
        .expect("exact-size local task should execute");

    assert!(result.is_success());
    assert_eq!(*counter.borrow(), 1);
}

#[test]
fn test_sequential_batch_executor_reports_start_failure() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(0))
        .build();

    let error = executor
        .execute_with_count([TestTask::succeed()], 1)
        .expect_err("start reporter failure should be returned");

    assert!(matches!(error, BatchExecutionError::ProgressReport { .. }));
}

#[test]
fn test_sequential_batch_executor_preserves_count_exceeded_when_failure_report_fails()
 {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(1))
        .build();

    let error = executor
        .execute_with_count([TestTask::succeed(), TestTask::succeed()], 1)
        .expect_err("count overflow should be returned");

    let BatchExecutionError::CountExceeded { report_error, .. } = error else {
        panic!("count overflow should remain the primary error");
    };
    assert!(report_error.is_some());
}

#[test]
fn test_sequential_batch_executor_reports_failure_terminal_error() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(1))
        .task_failure_policy(TaskFailurePolicy::Continue)
        .build();

    let error = executor
        .execute_with_count([TestTask::fail("failed")], 1)
        .expect_err("failure terminal reporter should fail");

    assert!(matches!(error, BatchExecutionError::ProgressReport { .. }));
}

#[test]
fn test_sequential_batch_executor_reports_stopped_policy_terminal_error() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(1))
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build();

    let error = executor
        .execute_with_count([TestTask::fail("failed")], 1)
        .expect_err("stopped-policy terminal reporter should fail");

    assert!(matches!(error, BatchExecutionError::ProgressReport { .. }));
}

#[test]
fn test_sequential_batch_executor_collects_failures_and_panics() {
    let executor = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::Continue)
        .build();
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic("panic in sequential batch"),
    ];

    let result = executor
        .execute_with_count(tasks, 3)
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
        .execute_with_count(tasks, 1)
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
        .execute_with_count(tasks, 3)
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_sequential_batch_executor_reports_count_exceeded() {
    let executor = SequentialBatchExecutor::new();
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute_with_count(tasks, 1)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
            ..
        } => {
            assert_eq!(expected, 1);
            assert_eq!(observed_at_least, 2);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_sequential_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingReporter::new());
    let executor = SequentialBatchExecutor::builder()
        .reporter_arc(reporter.clone())
        .report_interval(Duration::from_millis(10))
        .build();
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
        TestTask::sleep_success(Duration::from_millis(20)),
    ];

    let result = executor
        .execute_with_count(tasks, 3)
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

#[test]
fn test_sequential_batch_executor_reports_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingReporter::new());
    let executor = SequentialBatchExecutor::builder()
        .reporter_arc(reporter.clone())
        .report_interval(Duration::ZERO)
        .build();
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let result = executor
        .execute_with_count(tasks, 2)
        .expect("sequential batch should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 2);
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 2,
            active_count: 0,
            completed_count,
            ..
        } if *completed_count >= 1
    )));
}

#[test]
fn test_sequential_batch_executor_propagates_progress_reporter_start_panic() {
    const PANIC_MESSAGE: &str = "progress reporter start panic";
    let executor = SequentialBatchExecutor::builder()
        .reporter(PanickingReporter::new(
            ProgressPanicPhase::Start,
            PANIC_MESSAGE,
        ))
        .build();
    let tasks = vec![TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 1)
    }))
    .expect_err("progress reporter start panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_sequential_batch_executor_propagates_progress_reporter_process_panic() {
    const PANIC_MESSAGE: &str = "progress reporter process panic";
    let executor = SequentialBatchExecutor::builder()
        .reporter(PanickingReporter::new(
            ProgressPanicPhase::Process,
            PANIC_MESSAGE,
        ))
        .report_interval(Duration::from_nanos(1))
        .build();
    let tasks = vec![TestTask::sleep_success(Duration::from_millis(1))];

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 1)
    }))
    .expect_err("progress reporter process panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_sequential_batch_executor_propagates_progress_reporter_finish_panic() {
    const PANIC_MESSAGE: &str = "progress reporter finish panic";
    let executor = SequentialBatchExecutor::builder()
        .reporter(PanickingReporter::new(
            ProgressPanicPhase::Finish,
            PANIC_MESSAGE,
        ))
        .build();
    let tasks = vec![TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 1)
    }))
    .expect_err("progress reporter finish panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

/// Error type that intentionally does not implement [`std::fmt::Debug`].
struct NonDebugError;

/// Runnable task used to prove executor APIs do not require debug errors.
struct NonDebugTask;

impl Runnable<NonDebugError> for NonDebugTask {
    /// Runs successfully without constructing the non-debug error.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    fn run(&mut self) -> Result<(), NonDebugError> {
        Ok(())
    }
}

/// Non-`Send` task used to exercise the concrete sequential executor API.
struct LocalTask(Rc<RefCell<usize>>);

impl Runnable<&'static str> for LocalTask {
    /// Runs the task by updating local shared state.
    fn run(&mut self) -> Result<(), &'static str> {
        *self.0.borrow_mut() += 1;
        Ok(())
    }
}

/// Non-`Send` callable used to exercise the concrete sequential executor API.
struct LocalCallable(Rc<String>);

impl Callable<Rc<String>, Rc<String>> for LocalCallable {
    /// Returns the locally owned string.
    fn call(&mut self) -> Result<Rc<String>, Rc<String>> {
        Ok(Rc::clone(&self.0))
    }
}
