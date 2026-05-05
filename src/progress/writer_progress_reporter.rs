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
    io::Write,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use super::{
    ProgressCounters,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
    progress_format::{
        format_duration,
        progress_percent,
    },
};

/// Progress reporter that writes human-readable messages to a writer.
///
/// The output format follows the Java `PrinterProgressReporter` style closely
/// enough for console and log output to share the same lifecycle messages.
///
/// # Type Parameters
///
/// * `W` - Writer receiving formatted progress messages.
///
#[derive(Debug)]
pub struct WriterProgressReporter<W> {
    /// Shared writer receiving progress messages.
    writer: Arc<Mutex<W>>,
}

impl<W> WriterProgressReporter<W> {
    /// Creates a progress reporter from a shared writer.
    ///
    /// # Parameters
    ///
    /// * `writer` - Shared writer receiving formatted progress messages.
    ///
    /// # Returns
    ///
    /// A writer-backed progress reporter.
    #[inline]
    pub fn new(writer: Arc<Mutex<W>>) -> Self {
        Self { writer }
    }

    /// Creates a progress reporter from an owned writer.
    ///
    /// # Parameters
    ///
    /// * `writer` - Writer receiving formatted progress messages.
    ///
    /// # Returns
    ///
    /// A writer-backed progress reporter.
    #[inline]
    pub fn from_writer(writer: W) -> Self {
        Self::new(Arc::new(Mutex::new(writer)))
    }

    /// Returns the shared writer used by this reporter.
    ///
    /// # Returns
    ///
    /// A shared reference to the writer mutex.
    #[inline]
    pub fn writer(&self) -> &Arc<Mutex<W>> {
        &self.writer
    }
}

impl<W> WriterProgressReporter<W>
where
    W: Write + Send,
{
    /// Writes the batch-start message.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    ///
    /// # Panics
    ///
    /// Panics when writing to the configured writer fails.
    pub fn start(&self, total_count: usize) {
        self.write_line(format_args!("Starting {total_count} tasks..."));
    }

    /// Writes a progress snapshot.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    /// * `active_count` - Number of active tasks or chunks.
    /// * `completed_count` - Number of completed items or tasks.
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Panics
    ///
    /// Panics when writing to the configured writer fails.
    pub fn process(
        &self,
        total_count: usize,
        active_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) {
        self.write_line(format_args!(""));
        self.write_line(format_args!(
            "--------------------------------------------------"
        ));
        self.write_line(format_args!("Waiting for all batch tasks to finish..."));
        self.write_line(format_args!("Total tasks: {total_count}"));
        self.write_line(format_args!("Current active tasks: {active_count}"));
        self.write_line(format_args!("Current completed tasks: {completed_count}"));
        self.write_line(format_args!(
            "Current tasks in queue: {}",
            total_count.saturating_sub(completed_count + active_count)
        ));
        self.write_line(format_args!(
            "Progress: {:.2}%",
            progress_percent(completed_count, total_count)
        ));
        self.write_process_speed(
            completed_count,
            total_count.saturating_sub(completed_count),
            elapsed,
        );
    }

    /// Writes the batch-finish message.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    /// * `elapsed` - Total monotonic elapsed duration.
    ///
    /// # Panics
    ///
    /// Panics when writing to the configured writer fails.
    pub fn finish(&self, total_count: usize, elapsed: Duration) {
        self.write_line(format_args!("All {total_count} tasks are finished."));
        self.write_line(format_args!(
            "Processed {total_count} tasks in {}.",
            format_duration(elapsed)
        ));
    }
}

impl<W> ProgressReporter for WriterProgressReporter<W>
where
    W: Write + Send,
{
    /// Writes a progress event.
    ///
    /// # Parameters
    ///
    /// * `event` - Progress event to write.
    ///
    /// # Panics
    ///
    /// Panics when writing to the configured writer fails.
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

impl<W> WriterProgressReporter<W>
where
    W: Write,
{
    /// Writes a single line to the configured writer.
    ///
    /// # Parameters
    ///
    /// * `args` - Preformatted message arguments.
    ///
    /// # Panics
    ///
    /// Panics when the writer mutex is poisoned or writing fails.
    fn write_line(&self, args: std::fmt::Arguments<'_>) {
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        writeln!(writer, "{args}").expect("progress reporter should write one line");
    }

    /// Writes processing speed information.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of completed items or tasks.
    /// * `remaining_count` - Number of remaining items or tasks.
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Panics
    ///
    /// Panics when writing to the configured writer fails.
    fn write_process_speed(
        &self,
        completed_count: usize,
        remaining_count: usize,
        elapsed: Duration,
    ) {
        if completed_count == 0 {
            self.write_line(format_args!("No task processed."));
            return;
        }
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let speed = elapsed_ms / completed_count as f64;
        let tasks_per_minute =
            completed_count as f64 * 60.0 * 1000.0 / elapsed_ms.max(f64::EPSILON);
        self.write_line(format_args!(
            "Average speed: {speed:.2} ms/task, i.e., {tasks_per_minute:.2} tasks/min"
        ));
        let remaining = Duration::from_secs_f64(remaining_count as f64 * speed / 1000.0);
        self.write_line(format_args!(
            "Estimated remaining time: {}",
            format_duration(remaining)
        ));
    }
}
