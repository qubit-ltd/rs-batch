/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::WriterProgressReporter;
use std::{
    io::Cursor,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

#[test]
fn test_writer_progress_reporter_writes_lifecycle_messages() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::new())));
    let reporter = WriterProgressReporter::new(output.clone());
    assert!(Arc::ptr_eq(reporter.writer(), &output));

    reporter.start(3);
    reporter.process(3, 1, 2, Duration::from_millis(250));
    reporter.finish(3, Duration::from_secs(2));

    let bytes = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");
    assert!(text.contains("Starting 3 tasks..."));
    assert!(text.contains("Current active tasks: 1"));
    assert!(text.contains("Current completed tasks: 2"));
    assert!(text.contains("All 3 tasks are finished."));
    assert!(text.contains("Processed 3 tasks in 2.00s."));
}

#[test]
fn test_writer_progress_reporter_from_writer_exposes_writer() {
    let reporter = WriterProgressReporter::from_writer(Cursor::new(Vec::new()));

    reporter.start(1);
    reporter.finish(1, Duration::from_millis(1));

    let bytes = reporter
        .writer()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");
    assert!(text.contains("Starting 1 tasks..."));
    assert!(text.contains("Processed 1 tasks in 1ms."));
}
