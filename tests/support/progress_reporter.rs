// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Test progress reporters and panic payload helpers.

use std::{any::Any, panic::panic_any, sync::Mutex};

use qubit_progress::{Event as QubitProgressEvent, Phase, Reporter, ReporterError};

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
pub struct RecordingReporter {
    /// Recorded lifecycle events.
    events: Mutex<Vec<ProgressEvent>>,
}

/// Progress reporter that records raw lifecycle phases in memory.
#[derive(Debug, Default)]
pub struct PhaseRecordingReporter {
    /// Recorded lifecycle phases.
    phases: Mutex<Vec<Phase>>,
}

impl PhaseRecordingReporter {
    /// Creates an empty phase recording reporter.
    ///
    /// # Returns
    ///
    /// A reporter with no stored lifecycle phases.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns recorded lifecycle phases in callback order.
    ///
    /// # Returns
    ///
    /// A cloned list of lifecycle phases.
    pub fn phases(&self) -> Vec<Phase> {
        self.phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Reporter for PhaseRecordingReporter {
    /// Records the lifecycle phase carried by `event`.
    ///
    /// # Parameters
    ///
    /// * `event` - Progress event emitted by the executor under test.
    ///
    /// # Returns
    ///
    /// `Ok(())` after recording the event phase.
    fn report(&self, event: &QubitProgressEvent) -> Result<(), ReporterError> {
        self.phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.phase());
        Ok(())
    }
}

impl RecordingReporter {
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

impl Reporter for RecordingReporter {
    fn report(&self, event: &QubitProgressEvent) -> Result<(), ReporterError> {
        let counter = event
            .metrics()
            .first()
            .expect("batch progress event should contain one counter");
        let total_count = progress_count_to_usize(counter.total().unwrap_or(counter.completed()));
        let recorded = match event.phase() {
            Phase::Started => ProgressEvent::Start { total_count },
            Phase::Running => ProgressEvent::Process {
                total_count,
                active_count: progress_count_to_usize(counter.active()),
                completed_count: progress_count_to_usize(counter.completed()),
            },
            Phase::Succeeded | Phase::Failed | Phase::Cancelled => ProgressEvent::Finish {
                total_count,
                completed_count: progress_count_to_usize(counter.completed()),
            },
        };
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(recorded);
        Ok(())
    }
}

/// Converts a progress counter value into the platform test count type.
///
/// # Parameters
///
/// * `count` - Counter value reported by `qubit-progress`.
///
/// # Returns
///
/// The same count represented as `usize`.
fn progress_count_to_usize(count: u64) -> usize {
    usize::try_from(count).expect("test progress count should fit usize")
}

/// Progress reporter that panics from one configured lifecycle callback.
#[derive(Debug, Clone, Copy)]
pub struct PanickingReporter {
    /// Callback phase that should panic.
    phase: ProgressPanicPhase,
    /// Panic payload message.
    message: &'static str,
}

impl PanickingReporter {
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

impl Reporter for PanickingReporter {
    fn report(&self, event: &QubitProgressEvent) -> Result<(), ReporterError> {
        match event.phase() {
            Phase::Started => self.panic_if_configured(ProgressPanicPhase::Start),
            Phase::Running => self.panic_if_configured(ProgressPanicPhase::Process),
            Phase::Succeeded | Phase::Failed | Phase::Cancelled => {
                self.panic_if_configured(ProgressPanicPhase::Finish);
            }
        }
        Ok(())
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
