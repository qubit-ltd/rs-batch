/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use log::{
    LevelFilter,
    Log,
    Metadata,
    Record,
};
use qubit_batch::{
    LoggerProgressReporter,
    ProgressCounters,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
};
use std::sync::{
    Mutex,
    Once,
};
use std::time::Duration;

#[test]
fn test_logger_progress_reporter_writes_lifecycle_messages() {
    static LOGGER: RecordingLogger = RecordingLogger::new();
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("test logger should install once");
        log::set_max_level(LevelFilter::Info);
    });
    LOGGER.clear();

    let reporter = LoggerProgressReporter::new("qubit_batch_logger_progress_test")
        .with_level(log::Level::Warn);
    assert_eq!(reporter.target(), "qubit_batch_logger_progress_test");
    assert_eq!(reporter.level(), log::Level::Warn);

    reporter.start(2);
    reporter.process(2, 1, 1, Duration::from_millis(50));
    reporter.process(0, 0, 0, Duration::from_millis(50));
    reporter.finish(2, Duration::from_millis(100));
    for event in lifecycle_events() {
        reporter.report(&event);
    }

    let default_reporter = LoggerProgressReporter::default();
    assert_eq!(default_reporter.target(), "qubit_batch::progress");
    assert_eq!(default_reporter.level(), log::Level::Info);

    let messages = LOGGER.messages();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("qubit_batch_logger_progress_test: Starting 2 tasks")),
        "expected start log message: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| message
            .contains("qubit_batch_logger_progress_test: All 2 tasks are finished")),
        "expected finish log message: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| message
            .contains("qubit_batch_logger_progress_test: No task processed.")),
        "expected zero-progress log message: {messages:?}"
    );
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
        metadata.level() <= log::Level::Warn
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
