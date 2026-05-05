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
    fmt,
    time::Duration,
};

use qubit_progress::model::ProgressCounters;

use crate::{
    BatchOutcome,
    BatchTaskError,
    BatchTaskFailure,
};

/// Mutable state collected while a batch execution is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchExecutionState<E> {
    /// Declared task count for this batch.
    task_count: usize,
    /// Number of tasks currently in flight.
    active_count: usize,
    /// Number of tasks that reached a terminal outcome.
    completed_count: usize,
    /// Number of successful tasks.
    succeeded_count: usize,
    /// Number of tasks that returned errors.
    failed_count: usize,
    /// Number of tasks that panicked.
    panicked_count: usize,
    /// Detailed failures collected during execution.
    failures: Vec<BatchTaskFailure<E>>,
}

impl<E> BatchExecutionState<E> {
    /// Creates empty state for a declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared number of tasks in the batch.
    ///
    /// # Returns
    ///
    /// Empty execution state.
    pub const fn new(task_count: usize) -> Self {
        Self {
            task_count,
            active_count: 0,
            completed_count: 0,
            succeeded_count: 0,
            failed_count: 0,
            panicked_count: 0,
            failures: Vec::new(),
        }
    }

    /// Records that one task has started.
    pub fn record_task_started(&mut self) {
        self.active_count += 1;
    }

    /// Records one successful task completion.
    pub fn record_task_succeeded(&mut self) {
        self.active_count = self.active_count.saturating_sub(1);
        self.completed_count += 1;
        self.succeeded_count += 1;
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    pub fn record_task_failed(&mut self, index: usize, error: E)
    where
        E: fmt::Debug,
    {
        self.active_count = self.active_count.saturating_sub(1);
        self.completed_count += 1;
        self.failed_count += 1;
        self.failures
            .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
    }

    /// Records one task panic.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Captured task panic.
    pub fn record_task_panicked(&mut self, index: usize, error: BatchTaskError<E>) {
        self.active_count = self.active_count.saturating_sub(1);
        self.completed_count += 1;
        self.panicked_count += 1;
        self.failures.push(BatchTaskFailure::new(index, error));
    }

    /// Returns generic progress counters for this execution state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting. Panicked tasks are folded into
    /// the generic failed counter because panic is a batch-domain failure
    /// reason, not a separate progress dimension.
    pub const fn progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.task_count))
            .with_active_count(self.active_count)
            .with_completed_count(self.completed_count)
            .with_succeeded_count(self.succeeded_count)
            .with_failed_count(self.failed_count + self.panicked_count)
    }

    /// Consumes this state and builds a batch outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The final or partial outcome represented by this state.
    pub fn into_outcome(self, elapsed: Duration) -> BatchOutcome<E> {
        BatchOutcome::from_validated_parts(
            self.task_count,
            self.completed_count,
            self.succeeded_count,
            self.failed_count,
            self.panicked_count,
            elapsed,
            self.failures,
        )
    }

    /// Returns the declared task count.
    pub const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns the completed task count.
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }
}
