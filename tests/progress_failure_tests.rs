// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for [`ProgressFailure`](qubit_batch::ProgressFailure).

use std::error::Error;
use std::time::Duration;

use qubit_batch::ProgressFailure;
use qubit_progress::{
    AutoReporterError, CompletionError, EmissionError, Event, FinishError, Metric, Progress,
    Reporter, ReporterError, StartError,
};

#[derive(Debug)]
struct TerminalFailingReporter {
    remaining_successes: std::sync::atomic::AtomicUsize,
}

impl TerminalFailingReporter {
    const fn new() -> Self {
        Self {
            remaining_successes: std::sync::atomic::AtomicUsize::new(1),
        }
    }
}

impl Reporter for TerminalFailingReporter {
    fn report(&self, _event: &Event) -> Result<(), ReporterError> {
        if self
            .remaining_successes
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            != 0
        {
            Ok(())
        } else {
            Err(ReporterError::message("terminal report failed"))
        }
    }
}

#[test]
fn test_progress_failure_converts_start_error() {
    let failure = ProgressFailure::from(StartError::OperationIdExhausted);
    assert!(matches!(failure, ProgressFailure::Start(_)));
    assert!(!failure.to_string().is_empty());
}

#[test]
fn test_progress_failure_converts_emission_error() {
    let failure = ProgressFailure::from(EmissionError::SequenceExhausted);
    assert!(matches!(failure, ProgressFailure::Emission(_)));
    assert!(!failure.to_string().is_empty());
    assert!(Error::source(&failure).is_none());
}

#[test]
fn test_progress_failure_converts_auto_reporter_error() {
    let error = AutoReporterError::from(EmissionError::SequenceExhausted);
    let failure = ProgressFailure::from(error);
    assert!(matches!(failure, ProgressFailure::AutoReporter(_)));
}

#[test]
fn test_progress_failure_from_finish_error_maps_completion_and_terminal() {
    let incomplete = ProgressFailure::from_finish_error(FinishError::Incomplete {
        elapsed: Duration::ZERO,
        source: CompletionError::ActiveWork {
            metric_id: "tasks".into(),
            active: 1,
        },
    });
    assert!(matches!(incomplete, ProgressFailure::Completion(_)));

    let terminal_error = Progress::builder(&TerminalFailingReporter::new())
        .metric(Metric::new("tasks", "Tasks"))
        .start()
        .expect("progress start should succeed")
        .finish()
        .expect_err("terminal failure should be retained");
    let terminal = ProgressFailure::from_finish_error(terminal_error);
    assert!(matches!(terminal, ProgressFailure::Terminal(_)));
    assert!(terminal.elapsed().is_some());
    assert!(Error::source(&terminal).is_some());
}

#[test]
fn test_progress_failure_completion_error_display() {
    let failure = ProgressFailure::from_finish_error(FinishError::Incomplete {
        elapsed: Duration::from_millis(5),
        source: CompletionError::IncompleteTotal {
            metric_id: "tasks".into(),
            completed: 0,
            total: 1,
        },
    });
    assert_eq!(failure.elapsed(), None);
    assert!(!failure.to_string().is_empty());
}
