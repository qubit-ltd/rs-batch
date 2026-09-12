// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::Event;
use qubit_progress::NoopReporter;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

#[derive(Default)]
struct RecordingReporter(Mutex<Vec<Phase>>);

impl Reporter for RecordingReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        self.0
            .lock()
            .expect("phase log lock must be healthy")
            .push(event.phase());
        Ok(())
    }
}

fn coordinator() -> ParallelBatchExecutionCoordinator {
    ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO)
}

#[test]
fn source_consumption_proves_exhaustion() {
    let outcome = coordinator()
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                for token in source.by_ref() {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect("consuming source to None should complete the schedule");
    assert!(outcome.is_success());
}

#[test]
fn source_scheduler_return_without_exhaustion_is_incomplete() {
    let error = coordinator()
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                context.execute_task(source.next().expect("declared task should exist"));
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("the source must be exhausted explicitly");
    assert!(error.is_incomplete_schedule());
    assert_eq!(error.outcome().completed_count(), 1);
}

#[test]
fn source_scheduler_return_without_exhaustion_reports_failed_terminal() {
    let reporter = Arc::new(RecordingReporter::default());
    let coordinator = ParallelBatchExecutionCoordinator::new(reporter.clone(), Duration::ZERO);
    let error = coordinator
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                context.execute_task(source.next().expect("declared task should exist"));
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("the source must be observed to exhaustion");
    assert!(error.is_incomplete_schedule());
    assert_eq!(error.outcome().completed_count(), 1);
    let phases = reporter.0.lock().expect("phase log lock must be healthy");
    assert_eq!(phases.first(), Some(&Phase::Started));
    assert_eq!(phases.last(), Some(&Phase::Failed));
    assert!(!phases.contains(&Phase::Succeeded));
}

#[test]
fn empty_source_without_exhaustion_reports_failed_terminal() {
    let reporter = Arc::new(RecordingReporter::default());
    let coordinator = ParallelBatchExecutionCoordinator::new(reporter.clone(), Duration::ZERO);
    let error = coordinator
        .execute_with_source::<_, (), Infallible, _>(
            std::iter::empty::<()>(),
            0,
            TaskFailurePolicy::Continue,
            |_source, _context| Ok(()),
        )
        .expect_err("zero declared tasks do not prove source exhaustion");
    assert!(error.is_incomplete_schedule());
    let phases = reporter.0.lock().expect("phase log lock must be healthy");
    assert_eq!(phases.as_slice(), [Phase::Started, Phase::Failed]);
}

#[test]
fn exhausted_source_reports_succeeded_terminal() {
    let reporter = Arc::new(RecordingReporter::default());
    let coordinator = ParallelBatchExecutionCoordinator::new(reporter.clone(), Duration::ZERO);
    let outcome = coordinator
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                for token in source.by_ref() {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect("observed exhaustion completes the schedule");
    assert_eq!(outcome.completed_count(), 1);
    let phases = reporter.0.lock().expect("phase log lock must be healthy");
    assert_eq!(phases.first(), Some(&Phase::Started));
    assert_eq!(phases.last(), Some(&Phase::Succeeded));
    assert_eq!(phases.iter().filter(|phase| **phase == Phase::Succeeded).count(), 1);
    assert!(!phases.contains(&Phase::Failed));
}

#[test]
fn policy_stop_does_not_require_exhaustion_probe() {
    let reporter = Arc::new(RecordingReporter::default());
    let coordinator = ParallelBatchExecutionCoordinator::new(reporter.clone(), Duration::ZERO);
    let outcome = coordinator
        .execute_with_source(
            std::iter::repeat_with(|| || Err::<(), _>("stop")),
            8,
            TaskFailurePolicy::StopOnFirstFailure,
            |source, context| {
                context.execute_task(source.next().expect("first task should exist"));
                Ok::<(), Infallible>(())
            },
        )
        .expect("policy stop remains a normal outcome");
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
    let phases = reporter.0.lock().expect("phase log lock must be healthy");
    assert_eq!(phases.first(), Some(&Phase::Started));
    assert_eq!(phases.last(), Some(&Phase::Failed));
    assert!(!phases.contains(&Phase::Succeeded));
}

struct RejectFailedReporter;

impl Reporter for RejectFailedReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if event.phase() == Phase::Failed {
            Err(ReporterError::new(std::io::Error::other("terminal rejected")))
        } else {
            Ok(())
        }
    }
}

#[test]
fn source_exhaustion_error_keeps_terminal_report_failure_secondary() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(RejectFailedReporter), Duration::ZERO);
    let error = coordinator
        .execute_with_source::<_, (), Infallible, _>(
            std::iter::empty::<()>(),
            0,
            TaskFailurePolicy::Continue,
            |_source, _context| Ok(()),
        )
        .expect_err("source exhaustion remains the primary error");
    assert!(matches!(
        error,
        BatchExecutionError::IncompleteSchedule {
            report_error: Some(_),
            ..
        }
    ));
}

#[test]
fn source_is_fused_after_admission_stop() {
    let outcome = coordinator()
        .execute_with_source(
            std::iter::repeat_with(|| || Err::<(), _>("stop")),
            8,
            TaskFailurePolicy::StopOnFirstFailure,
            |source, context| {
                if let Some(token) = source.next() {
                    context.execute_task(token);
                }
                assert!(source.next().is_none());
                Ok::<(), Infallible>(())
            },
        )
        .expect("policy stop returns an outcome");
    assert_eq!(outcome.failed_count(), 1);
}
