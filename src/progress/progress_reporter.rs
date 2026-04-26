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
/// # Author
///
/// Haixing Hu
pub trait ProgressReporter: Send + Sync {
    /// Reports that a batch execution has started.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared task count for the batch.
    fn start(&self, total_count: usize);

    /// Reports batch execution progress.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared task count for the batch.
    /// * `active_count` - Number of tasks that are currently in flight.
    /// * `completed_count` - Number of tasks that have completed.
    /// * `elapsed` - Elapsed wall-clock time since batch start.
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
    /// * `elapsed` - Total elapsed wall-clock time.
    fn finish(&self, total_count: usize, elapsed: Duration);
}
