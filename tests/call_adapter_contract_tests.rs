// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public callable adapter and scheduler-error mapping contracts.
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchOutcome;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_batch::execute::spi::call_with_executor;
use qubit_function::Runnable;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

#[derive(Debug)]
struct NonClone {
    value: i32,
}

#[test]
fn test_mapping_keeps_non_clone_outputs() {
    let error = SequentialBatchExecutor::new()
        .call_with_count([|| Ok::<_, ()>(NonClone { value: 42 })], 2)
        .expect_err("declared count exceeds source");
    let mapped = error.map_scheduler_error::<io::Error, _>(|never| match never {});
    assert_eq!(mapped.outcome().completed_count(), 1);
    assert!(mapped.source().is_count_shortfall());
    assert_eq!(mapped.outputs()[0].index(), 0);
    assert_eq!(mapped.outputs()[0].value().value, 42);
}

#[test]
fn test_helper_collects_in_source_order() {
    let executor = SequentialBatchExecutor::new();
    let result = call_with_executor(&executor, (0..3).map(|i| move || Ok::<_, ()>(i)), 3).expect("valid source");
    let values: Vec<_> = result.outputs().iter().map(|o| *o.value()).collect();
    assert_eq!(values, vec![0, 1, 2]);
}

/// Rejects only terminal progress, preserving the scheduler error as primary.
struct TerminalFailure;
impl Reporter for TerminalFailure {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if matches!(event.phase(), Phase::Succeeded | Phase::Failed) {
            Err(ReporterError::new(io::Error::other("terminal rejected")))
        } else {
            Ok(())
        }
    }
}

/// Completes one token, then reports a scheduler rejection.
struct RejectScheduler;
impl BatchExecutor for RejectScheduler {
    type SchedulerError = io::Error;
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        ParallelBatchExecutionCoordinator::new(Arc::new(TerminalFailure), Duration::from_secs(60)).execute(
            tasks,
            count,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                let mut tasks = tasks.into_iter();
                if let Some(token) = context.next_task(&mut tasks) {
                    context.execute_task(token);
                }
                Err(io::Error::other("schedule rejected"))
            },
        )
    }
}

#[test]
fn test_mapping_preserves_secondary_reporter_failure() {
    let error = RejectScheduler
        .call_with_count([|| Ok::<_, ()>(NonClone { value: 7 })], 1)
        .expect_err("scheduler rejects after completing a callable");
    let calls = Cell::new(0);
    let mapped = error.map_scheduler_error(|source| {
        calls.set(calls.get() + 1);
        io::Error::new(io::ErrorKind::Interrupted, source)
    });
    assert_eq!(calls.get(), 1);
    assert_eq!(
        mapped.source().scheduler_error().expect("scheduler source").kind(),
        io::ErrorKind::Interrupted
    );
    assert!(mapped.source().progress_report_error().is_some());
    assert_eq!(mapped.outcome().completed_count(), 1);
    assert_eq!(mapped.outputs()[0].value().value, 7);
}

#[test]
fn test_non_scheduler_errors_do_not_invoke_mapping() {
    let errors = [
        SequentialBatchExecutor::new()
            .call_with_count([|| Ok::<_, ()>(1)], 0)
            .expect_err("overflow"),
        SequentialBatchExecutor::builder()
            .reporter(TerminalFailure)
            .build()
            .call_with_count([|| Ok::<_, ()>(1)], 1)
            .expect_err("terminal failure"),
    ];
    for error in errors {
        let completed = error.outcome().completed_count();
        let mapped = error.map_scheduler_error::<io::Error, _>(|_: Infallible| panic!("not a scheduler error"));
        assert_eq!(mapped.outcome().completed_count(), completed);
        assert_eq!(mapped.outputs().len(), completed);
    }
}

/// Source conversion must happen only after execution accepts the source.
struct ObserveIntoIter<'a> {
    entered: &'a Cell<bool>,
}
impl IntoIterator for ObserveIntoIter<'_> {
    type Item = fn() -> Result<(), ()>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.entered.set(true);
        Vec::new().into_iter()
    }
}
struct RejectBeforeSource;
impl BatchExecutor for RejectBeforeSource {
    type SchedulerError = io::Error;
    fn execute_with_count<T, E, I>(
        &self,
        _tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        Err(BatchExecutionError::ScheduleFailed {
            source: io::Error::other("rejected before source"),
            outcome: BatchOutcomeBuilder::builder(count).build().expect("empty outcome"),
            report_error: None,
        })
    }
}
#[test]
fn test_helper_defers_source_conversion_until_execution() {
    let entered = Cell::new(false);
    let error = call_with_executor(&RejectBeforeSource, ObserveIntoIter { entered: &entered }, 0)
        .expect_err("scheduler rejects without consuming source");
    assert!(error.source().is_schedule_failed());
    assert!(!entered.get());
}
