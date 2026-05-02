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
    /// Writes the batch-start message to standard output.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    fn start(&self, total_count: usize) {
        self.inner.start(total_count);
    }

    /// Writes a progress snapshot to standard output.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    /// * `active_count` - Number of active tasks or chunks.
    /// * `completed_count` - Number of completed items or tasks.
    /// * `elapsed` - Monotonic elapsed duration.
    fn process(
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
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    /// * `elapsed` - Total monotonic elapsed duration.
    fn finish(&self, total_count: usize, elapsed: Duration) {
        self.inner.finish(total_count, elapsed);
    }
}
