/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Shared test support for `qubit-batch`.

use std::{
    sync::Mutex,
    time::Duration,
};

use qubit_batch::ProgressReporter;

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
        /// Elapsed time since batch start.
        elapsed: Duration,
    },
    /// Batch finish notification.
    Finish {
        /// Declared task count.
        total_count: usize,
        /// Total elapsed time.
        elapsed: Duration,
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
    fn start(&self, total_count: usize) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Start { total_count });
    }

    fn process(
        &self,
        total_count: usize,
        active_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Process {
                total_count,
                active_count,
                completed_count,
                elapsed,
            });
    }

    fn finish(&self, total_count: usize, elapsed: Duration) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Finish {
                total_count,
                elapsed,
            });
    }
}
