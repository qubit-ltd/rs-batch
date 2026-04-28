/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::time::Duration;

/// Reports progress for one batch execution.
///
/// This trait mirrors the start / process / finish lifecycle used by the Java
/// `ParallelExecutor`, while adapting the time type to Rust's
/// [`Duration`](std::time::Duration).
///
/// Reporter callbacks are intentionally outside task failure aggregation. If a
/// reporter callback panics, the executor propagates that panic to the caller
/// instead of converting it into a [`crate::BatchTaskError`].
///
/// # Author
///
/// Haixing Hu
pub trait ProgressReporter: Send + Sync {
    /// Reports that a batch execution has started.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared task count for the batch.
    ///
    /// # Panics
    ///
    /// Any panic from this callback is propagated by the executor.
    fn start(&self, total_count: usize);

    /// Reports batch execution progress.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared task count for the batch.
    /// * `active_count` - Number of tasks that are currently in flight.
    /// * `completed_count` - Number of tasks that have completed.
    /// * `elapsed` - Monotonic elapsed duration since batch start.
    ///
    /// # Panics
    ///
    /// Any panic from this callback is propagated by the executor.
    fn process(
        &self,
        total_count: usize,
        active_count: usize,
        completed_count: usize,
        elapsed: Duration,
    );

    /// Reports that a batch execution has finished.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared task count for the batch.
    /// * `elapsed` - Total monotonic elapsed duration.
    ///
    /// # Panics
    ///
    /// Any panic from this callback is propagated by the executor.
    fn finish(&self, total_count: usize, elapsed: Duration);
}
