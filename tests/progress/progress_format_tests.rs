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
fn test_progress_format_percent_and_duration_through_writer_output() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::new())));
    let reporter = WriterProgressReporter::new(output.clone());

    reporter.process(4, 1, 2, Duration::from_millis(250));
    reporter.process(0, 0, 0, Duration::from_secs(2));

    let bytes = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_ref()
        .clone();
    let text = String::from_utf8(bytes).expect("progress output should be UTF-8");

    assert!(text.contains("Progress: 50.00%"));
    assert!(text.contains("Average speed: 125.00 ms/task"));
    assert!(text.contains("No task processed."));
}
