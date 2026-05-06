/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Test progress reporters and panic payload helpers.

use std::{
    any::Any,
    panic::panic_any,
    sync::Mutex,
};

use qubit_batch::{
    ProgressEvent as QubitProgressEvent,
    ProgressPhase,
    ProgressReporter,
};

/// Progress callback that should panic during a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressPanicPhase {
    /// Panic from a started progress event.
    Start,
    /// Panic from a running progress event.
    Process,
    /// Panic from a terminal progress event.
    Finish,
}

/// Recorded progress event produced by a test reporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressEvent {
    /// Batch start notification.
    Start {
        /// Declared task count.
        total_count: usize,
    },
    /// In-flight progress notification.
    Process {
        /// Declared task count.
        total_count: usize,
        /// Number of active tasks at callback time.
        active_count: usize,
        /// Number of completed tasks at callback time.
        completed_count: usize,
    },
    /// Batch finish notification.
    Finish {
        /// Declared task count.
        total_count: usize,
        /// Number of completed tasks at callback time.
        completed_count: usize,
    },
}

/// Progress reporter that records all callbacks in memory.
#[derive(Debug, Default)]
pub struct RecordingProgressReporter {
    /// Recorded lifecycle events.
    events: Mutex<Vec<ProgressEvent>>,
}

impl RecordingProgressReporter {
    /// Creates an empty recording reporter.
    ///
    /// # Returns
    ///
    /// A recording reporter with no stored events.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all recorded progress events.
    ///
    /// # Returns
    ///
    /// A cloned list of progress events in callback order.
    pub fn events(&self) -> Vec<ProgressEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ProgressReporter for RecordingProgressReporter {
    fn report(&self, event: &QubitProgressEvent) {
        let counters = event.counters();
        let total_count = counters.total_count().unwrap_or(counters.completed_count());
        let recorded = match event.phase() {
            ProgressPhase::Started => ProgressEvent::Start { total_count },
            ProgressPhase::Running => ProgressEvent::Process {
                total_count,
                active_count: counters.active_count(),
                completed_count: counters.completed_count(),
            },
            ProgressPhase::Finished | ProgressPhase::Failed | ProgressPhase::Canceled => {
                ProgressEvent::Finish {
                    total_count,
                    completed_count: counters.completed_count(),
                }
            }
        };
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(recorded);
    }
}

/// Progress reporter that panics from one configured lifecycle callback.
#[derive(Debug, Clone, Copy)]
pub struct PanickingProgressReporter {
    /// Callback phase that should panic.
    phase: ProgressPanicPhase,
    /// Panic payload message.
    message: &'static str,
}

impl PanickingProgressReporter {
    /// Creates a reporter that panics from `phase`.
    ///
    /// # Parameters
    ///
    /// * `phase` - Callback phase that should panic.
    /// * `message` - Panic payload message.
    ///
    /// # Returns
    ///
    /// A panicking progress reporter.
    pub const fn new(phase: ProgressPanicPhase, message: &'static str) -> Self {
        Self { phase, message }
    }

    /// Panics when `phase` matches this reporter's configured phase.
    ///
    /// # Parameters
    ///
    /// * `phase` - Current callback phase.
    ///
    /// # Panics
    ///
    /// Panics with this reporter's configured message when `phase` matches.
    fn panic_if_configured(&self, phase: ProgressPanicPhase) {
        if self.phase == phase {
            panic_any(self.message);
        }
    }
}

impl ProgressReporter for PanickingProgressReporter {
    fn report(&self, event: &QubitProgressEvent) {
        match event.phase() {
            ProgressPhase::Started => self.panic_if_configured(ProgressPanicPhase::Start),
            ProgressPhase::Running => self.panic_if_configured(ProgressPanicPhase::Process),
            ProgressPhase::Finished | ProgressPhase::Failed | ProgressPhase::Canceled => {
                self.panic_if_configured(ProgressPanicPhase::Finish);
            }
        }
    }
}

/// Extracts a string message from a panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// `Some(message)` for `&'static str` and `String` payloads, or `None` for
/// other payload types.
pub fn panic_payload_message(payload: &(dyn Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some(*message)
    } else {
        payload.downcast_ref::<String>().map(String::as_str)
    }
}
