/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::Duration;

use super::{
    ProgressReporter,
    progress_format::{
        format_duration,
        progress_percent,
    },
};

/// Progress reporter that writes messages through the `log` crate.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggerProgressReporter {
    /// Log target used for emitted records.
    target: String,
    /// Log level used for emitted records.
    level: log::Level,
}

impl LoggerProgressReporter {
    /// Creates a logger progress reporter at [`log::Level::Info`].
    ///
    /// # Parameters
    ///
    /// * `target` - Log target used for emitted records.
    ///
    /// # Returns
    ///
    /// A logger-backed progress reporter.
    #[inline]
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            level: log::Level::Info,
        }
    }

    /// Returns a copy configured to use `level`.
    ///
    /// # Parameters
    ///
    /// * `level` - Log level used for emitted records.
    ///
    /// # Returns
    ///
    /// This reporter configured with `level`.
    #[inline]
    pub fn with_level(self, level: log::Level) -> Self {
        Self { level, ..self }
    }

    /// Returns the configured log target.
    ///
    /// # Returns
    ///
    /// The log target used for emitted records.
    #[inline]
    pub fn target(&self) -> &str {
        self.target.as_str()
    }

    /// Returns the configured log level.
    ///
    /// # Returns
    ///
    /// The log level used for emitted records.
    #[inline]
    pub const fn level(&self) -> log::Level {
        self.level
    }

    /// Emits one message through the `log` crate.
    ///
    /// # Parameters
    ///
    /// * `message` - Message to emit.
    fn log_line(&self, message: String) {
        log::log!(target: self.target.as_str(), self.level, "{message}");
    }

    /// Emits processing speed information through the `log` crate.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of completed items or tasks.
    /// * `remaining_count` - Number of remaining items or tasks.
    /// * `elapsed` - Monotonic elapsed duration.
    fn log_process_speed(&self, completed_count: usize, remaining_count: usize, elapsed: Duration) {
        if completed_count == 0 {
            self.log_line("No task processed.".to_owned());
            return;
        }
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let speed = elapsed_ms / completed_count as f64;
        let tasks_per_minute =
            completed_count as f64 * 60.0 * 1000.0 / elapsed_ms.max(f64::EPSILON);
        self.log_line(format!(
            "Average speed: {speed:.2} ms/task, i.e., {tasks_per_minute:.2} tasks/min"
        ));
        let remaining = Duration::from_secs_f64(remaining_count as f64 * speed / 1000.0);
        self.log_line(format!(
            "Estimated remaining time: {}",
            format_duration(remaining)
        ));
    }
}

impl Default for LoggerProgressReporter {
    /// Creates a logger progress reporter with the default log target.
    ///
    /// # Returns
    ///
    /// A logger-backed progress reporter at [`log::Level::Info`].
    fn default() -> Self {
        Self::new("qubit_batch::progress")
    }
}

impl ProgressReporter for LoggerProgressReporter {
    /// Logs the batch-start message.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    fn start(&self, total_count: usize) {
        self.log_line(format!("Starting {total_count} tasks..."));
    }

    /// Logs a progress snapshot.
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
        self.log_line("--------------------------------------------------".to_owned());
        self.log_line("Waiting for all batch tasks to finish...".to_owned());
        self.log_line(format!("Total tasks: {total_count}"));
        self.log_line(format!("Current active tasks: {active_count}"));
        self.log_line(format!("Current completed tasks: {completed_count}"));
        self.log_line(format!(
            "Current tasks in queue: {}",
            total_count.saturating_sub(completed_count + active_count)
        ));
        self.log_line(format!(
            "Progress: {:.2}%",
            progress_percent(completed_count, total_count)
        ));
        self.log_process_speed(
            completed_count,
            total_count.saturating_sub(completed_count),
            elapsed,
        );
    }

    /// Logs the batch-finish message.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    /// * `elapsed` - Total monotonic elapsed duration.
    fn finish(&self, total_count: usize, elapsed: Duration) {
        self.log_line(format!("All {total_count} tasks are finished."));
        self.log_line(format!(
            "Processed {total_count} tasks in {}.",
            format_duration(elapsed)
        ));
    }
}
