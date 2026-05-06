/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for progress reporter re-exports.

use std::{
    io::Cursor,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use qubit_batch::{
    LoggerProgressReporter,
    NoOpProgressReporter,
    ProgressCounters,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
    WriterProgressReporter,
};

#[test]
fn test_batch_reexports_progress_reporter_implementations_from_rs_progress() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::<u8>::new())));
    let writer: qubit_progress::reporter::WriterProgressReporter<Cursor<Vec<u8>>> =
        WriterProgressReporter::new(output);
    let logger: qubit_progress::reporter::LoggerProgressReporter =
        LoggerProgressReporter::new("qubit_batch_progress_reexport");
    let no_op: qubit_progress::reporter::NoOpProgressReporter = NoOpProgressReporter;

    let _: &dyn qubit_progress::reporter::ProgressReporter = &writer;
    let _: &dyn qubit_progress::reporter::ProgressReporter = &logger;
    let _: &dyn qubit_progress::reporter::ProgressReporter = &no_op;
}

#[test]
fn test_writer_progress_reporter_reports_rs_progress_events() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::new())));
    let reporter = WriterProgressReporter::new(output.clone());
    assert!(Arc::ptr_eq(reporter.writer(), &output));

    reporter.report(&ProgressEvent::running(
        ProgressCounters::new(Some(4))
            .with_active_count(1)
            .with_completed_count(2),
        Duration::from_millis(250),
    ));

    let bytes = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");
    assert!(text.contains("running"));
    assert!(text.contains("2/4"));
    assert!(text.contains("50.00%"));
    assert!(text.contains("active 1"));
}

#[test]
fn test_writer_progress_reporter_from_writer_reports_rs_progress_events() {
    let reporter = WriterProgressReporter::from_writer(Cursor::new(Vec::new()));

    reporter.report(&ProgressEvent::finished(
        ProgressCounters::new(Some(1)).with_completed_count(1),
        Duration::from_millis(1),
    ));

    let bytes = reporter
        .writer()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");
    assert!(text.contains("finished"));
    assert!(text.contains("1/1"));
}

#[test]
fn test_no_op_progress_reporter_reports_all_event_phases() {
    let reporter = NoOpProgressReporter;
    for event in lifecycle_events() {
        reporter.report(&event);
    }
    assert_eq!(reporter, NoOpProgressReporter);
}

#[test]
fn test_writer_progress_reporter_reports_all_event_phases() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::new())));
    let reporter = WriterProgressReporter::new(output.clone());

    for event in lifecycle_events() {
        reporter.report(&event);
    }

    let bytes = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");
    assert!(text.contains("started"));
    assert!(text.contains("running"));
    assert!(text.contains("finished"));
    assert!(text.contains("failed"));
    assert!(text.contains("canceled"));
    assert!(text.contains("2/4"));
}

fn lifecycle_events() -> Vec<ProgressEvent> {
    [
        ProgressPhase::Started,
        ProgressPhase::Running,
        ProgressPhase::Finished,
        ProgressPhase::Failed,
        ProgressPhase::Canceled,
    ]
    .into_iter()
    .map(|phase| {
        ProgressEvent::builder()
            .phase(phase)
            .counters(
                ProgressCounters::new(Some(4))
                    .with_active_count(1)
                    .with_completed_count(2),
            )
            .elapsed(Duration::from_millis(250))
            .build()
    })
    .collect()
}
