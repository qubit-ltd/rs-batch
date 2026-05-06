/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::sync::{
    Arc,
    mpsc::{
        self,
        RecvTimeoutError,
    },
};
use std::time::{
    Duration,
    Instant,
};

use qubit_atomic::AtomicCount;
use qubit_progress::Progress;

use crate::{
    ProgressCounters,
    ProgressPhase,
    ProgressReporter,
};

/// Shared progress counters for a running parallel batch.
pub(crate) struct ParallelBatchProgressState {
    /// Number of tasks currently running.
    active_count: AtomicCount,
    /// Number of tasks that reached a terminal outcome.
    completed_count: AtomicCount,
    /// Number of successful tasks.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
}

impl ParallelBatchProgressState {
    /// Creates fresh progress state for one batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters.
    pub(crate) fn new() -> Self {
        Self {
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
        }
    }

    /// Returns the number of completed tasks.
    ///
    /// # Returns
    ///
    /// The completed task counter.
    pub(crate) fn completed_count(&self) -> usize {
        self.completed_count.get()
    }

    /// Builds generic progress counters from current state.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared total task count.
    ///
    /// # Returns
    ///
    /// Progress counters suitable for reporter events.
    pub(crate) fn progress_counters(&self, total_count: usize) -> ProgressCounters {
        ProgressCounters::new(Some(total_count))
            .with_active_count(self.active_count.get())
            .with_completed_count(self.completed_count.get())
            .with_succeeded_count(self.succeeded_count.get())
            .with_failed_count(self.failed_count.get() + self.panicked_count.get())
    }

    /// Records that one task has started.
    pub(crate) fn record_task_started(&self) {
        self.active_count.inc();
    }

    /// Records one successful task completion.
    pub(crate) fn record_task_succeeded(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.succeeded_count.inc();
    }

    /// Records one task error.
    pub(crate) fn record_task_failed(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.failed_count.inc();
    }

    /// Records one task panic.
    pub(crate) fn record_task_panicked(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.panicked_count.inc();
    }
}

/// Runs the periodic progress loop for one parallel batch execution.
///
/// # Parameters
///
/// * `reporter` - Reporter receiving progress callbacks.
/// * `state` - Shared batch state read by the reporting loop.
/// * `total_count` - Declared task count for the batch.
/// * `start` - Batch start time.
/// * `report_interval` - Delay between progress callbacks.
/// * `stop_receiver` - Stop signal receiver used by the caller thread.
pub(crate) fn run_progress_loop(
    reporter: Arc<dyn ProgressReporter>,
    state: Arc<ParallelBatchProgressState>,
    total_count: usize,
    start: Instant,
    report_interval: Duration,
    stop_receiver: mpsc::Receiver<()>,
) {
    let progress = Progress::from_start(reporter.as_ref(), report_interval, start);
    loop {
        match stop_receiver.recv_timeout(report_interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => progress.report_with_elapsed(
                ProgressPhase::Running,
                state.progress_counters(total_count),
                start.elapsed(),
            ),
        }
    }
}
