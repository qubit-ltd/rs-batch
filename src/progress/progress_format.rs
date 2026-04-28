/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::time::Duration;

/// Computes a progress percentage, treating an empty batch as complete.
///
/// # Parameters
///
/// * `completed_count` - Number of completed items or tasks.
/// * `total_count` - Declared item or task count.
///
/// # Returns
///
/// Percentage in the inclusive range `0.0..=100.0` for normal counters.
pub(super) fn progress_percent(completed_count: usize, total_count: usize) -> f64 {
    if total_count == 0 {
        100.0
    } else {
        completed_count as f64 * 100.0 / total_count as f64
    }
}

/// Formats a duration for human-readable progress output.
///
/// # Parameters
///
/// * `duration` - Duration to format.
///
/// # Returns
///
/// A compact duration string.
pub(super) fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{}ms", duration.as_millis())
    }
}
