/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for concrete progress reporters.

use std::{
    io::Cursor,
    sync::{
        Arc,
        Mutex,
        Once,
    },
    time::Duration,
};

use log::{
    LevelFilter,
    Log,
    Metadata,
    Record,
};
use qubit_batch::{
    ConsoleProgressReporter,
    LoggerProgressReporter,
    ProgressReporter,
    WriterProgressReporter,
};

#[test]
fn test_writer_progress_reporter_writes_lifecycle_messages() {
    let output = Arc::new(Mutex::new(Cursor::new(Vec::new())));
    let reporter = WriterProgressReporter::new(output.clone());
    assert!(Arc::ptr_eq(reporter.writer(), &output));

    reporter.start(3);
    reporter.process(3, 1, 2, Duration::from_millis(250));
    reporter.process(0, 0, 0, Duration::from_millis(250));
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
    assert!(text.contains("Progress: 100.00%"));
    assert!(text.contains("No task processed."));
    assert!(text.contains("All 3 tasks are finished."));
    assert!(text.contains("Processed 3 tasks in 2.00s."));
}

#[test]
fn test_writer_progress_reporter_from_writer_writes_lifecycle_messages() {
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

#[test]
fn test_console_progress_reporter_can_be_created() {
    let reporter = ConsoleProgressReporter::new();
    reporter.start(0);
    reporter.process(0, 0, 0, Duration::ZERO);
    reporter.finish(0, Duration::ZERO);

    let reporter = ConsoleProgressReporter::default();
    reporter.start(1);
    reporter.finish(1, Duration::from_secs(1));
}

#[test]
fn test_logger_progress_reporter_writes_to_log() {
    static LOGGER: RecordingLogger = RecordingLogger::new();
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("test logger should install once");
        log::set_max_level(LevelFilter::Info);
    });
    LOGGER.clear();

    let reporter = LoggerProgressReporter::new("qubit_batch_progress_test");
    assert_eq!(reporter.target(), "qubit_batch_progress_test");
    assert_eq!(reporter.level(), log::Level::Info);
    reporter.start(2);
    reporter.process(2, 1, 1, Duration::from_millis(50));
    reporter.process(0, 0, 0, Duration::from_millis(50));
    reporter.finish(2, Duration::from_millis(100));

    let default_reporter = LoggerProgressReporter::default();
    assert_eq!(default_reporter.target(), "qubit_batch::progress");
    assert_eq!(default_reporter.level(), log::Level::Info);

    let warn_reporter =
        LoggerProgressReporter::new("qubit_batch_progress_warn").with_level(log::Level::Warn);
    assert_eq!(warn_reporter.level(), log::Level::Warn);
    warn_reporter.start(1);

    let messages = LOGGER.messages();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("qubit_batch_progress_test: Starting 2 tasks...")),
        "expected start log message: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| message
            .contains("qubit_batch_progress_test: All 2 tasks are finished.")),
        "expected finish log message: {messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("qubit_batch_progress_test: No task processed.")),
        "expected zero-progress log message: {messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("qubit_batch_progress_warn: Starting 1 tasks...")),
        "expected warn log message: {messages:?}"
    );
}

struct RecordingLogger {
    messages: Mutex<Vec<String>>,
}

impl RecordingLogger {
    const fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
        }
    }

    fn clear(&self) {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn messages(&self) -> Vec<String> {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Log for RecordingLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("{}: {}", record.target(), record.args()));
        }
    }

    fn flush(&self) {}
}
