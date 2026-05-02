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

use super::ProgressReporter;

/// Progress reporter that intentionally does nothing.
///
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoOpProgressReporter;

impl ProgressReporter for NoOpProgressReporter {
    /// Ignores the batch-start notification.
    ///
    /// # Parameters
    ///
    /// * `_total_count` - Declared task count for the batch.
    fn start(&self, _total_count: usize) {}

    /// Ignores the batch-progress notification.
    ///
    /// # Parameters
    ///
    /// * `_total_count` - Declared task count for the batch.
    /// * `_active_count` - Number of tasks currently in flight.
    /// * `_completed_count` - Number of tasks that have completed.
    /// * `_elapsed` - Monotonic elapsed duration since batch start.
    fn process(
        &self,
        _total_count: usize,
        _active_count: usize,
        _completed_count: usize,
        _elapsed: Duration,
    ) {
    }

    /// Ignores the batch-finish notification.
    ///
    /// # Parameters
    ///
    /// * `_total_count` - Declared task count for the batch.
    /// * `_elapsed` - Total monotonic elapsed duration.
    fn finish(&self, _total_count: usize, _elapsed: Duration) {}
}
