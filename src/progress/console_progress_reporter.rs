/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    io,
    time::Duration,
};

use super::{
    ProgressCounters,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
    WriterProgressReporter,
};

/// Progress reporter that writes to standard output.
///
#[derive(Debug)]
pub struct ConsoleProgressReporter {
    /// Writer-backed reporter using standard output.
    inner: WriterProgressReporter<io::Stdout>,
}

impl ConsoleProgressReporter {
    /// Creates a console progress reporter.
    ///
    /// # Returns
    ///
    /// A reporter that writes progress messages to standard output.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Writes the batch-start message to standard output.
    pub fn start(&self, total_count: usize) {
        self.inner.start(total_count);
    }

    /// Writes a progress snapshot to standard output.
    pub fn process(
        &self,
        total_count: usize,
        active_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) {
        self.inner
            .process(total_count, active_count, completed_count, elapsed);
    }

    /// Writes the batch-finish message to standard output.
    pub fn finish(&self, total_count: usize, elapsed: Duration) {
        self.inner.finish(total_count, elapsed);
    }
}

impl Default for ConsoleProgressReporter {
    /// Creates a console progress reporter.
    ///
    /// # Returns
    ///
    /// A reporter that writes progress messages to standard output.
    fn default() -> Self {
        Self {
            inner: WriterProgressReporter::from_writer(io::stdout()),
        }
    }
}

impl ProgressReporter for ConsoleProgressReporter {
    /// Writes one progress event to standard output.
    ///
    /// # Parameters
    ///
    /// * `event` - Progress event to write.
    fn report(&self, event: &ProgressEvent) {
        let counters: ProgressCounters = event.counters();
        let total_count = counters.total_count().unwrap_or(counters.completed_count());
        match event.phase() {
            ProgressPhase::Started => self.start(total_count),
            ProgressPhase::Running => self.process(
                total_count,
                counters.active_count(),
                counters.completed_count(),
                event.elapsed(),
            ),
            ProgressPhase::Finished => self.finish(total_count, event.elapsed()),
            ProgressPhase::Failed | ProgressPhase::Canceled => self.process(
                total_count,
                counters.active_count(),
                counters.completed_count(),
                event.elapsed(),
            ),
        }
    }
}
