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
    Mutex,
    MutexGuard,
};
use std::time::Duration;

use qubit_atomic::AtomicCount;
use qubit_progress::model::ProgressCounters;

use crate::{
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
};

/// Shared state collected while a batch executor is running.
pub(crate) struct BatchExecutionState<E> {
    /// Declared task count.
    task_count: usize,
    /// Number of tasks observed from the source.
    observed_count: AtomicCount,
    /// Number of tasks currently running.
    active_count: AtomicCount,
    /// Number of tasks that reached a terminal outcome.
    completed_count: AtomicCount,
    /// Number of tasks that completed successfully.
    succeeded_count: AtomicCount,
    /// Number of tasks that returned their own errors.
    failed_count: AtomicCount,
    /// Number of tasks that panicked.
    panicked_count: AtomicCount,
    /// Detailed failures collected during execution.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> BatchExecutionState<E> {
    /// Creates empty execution state for a declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared number of tasks in the batch.
    ///
    /// # Returns
    ///
    /// Empty execution state.
    #[inline]
    pub(crate) const fn new(task_count: usize) -> Self {
        Self {
            task_count,
            observed_count: AtomicCount::zero(),
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Records one observed task.
    ///
    /// # Returns
    ///
    /// The observed task count after this task was recorded.
    #[inline]
    pub(crate) fn record_task_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records that one task has started.
    #[inline]
    pub(crate) fn record_task_started(&self) {
        self.active_count.inc();
    }

    /// Records one successful task completion.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[inline]
    pub(crate) fn record_task_succeeded(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.succeeded_count.inc();
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[inline]
    pub(crate) fn record_task_failed(&self, index: usize, error: E) {
        self.active_count.dec();
        self.completed_count.inc();
        self.failed_count.inc();
        Self::lock_failures(&self.failures)
            .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
    }

    /// Records one task panic.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Captured task panic.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[inline]
    pub(crate) fn record_task_panicked(&self, index: usize, error: BatchTaskError<E>) {
        self.active_count.dec();
        self.completed_count.inc();
        self.panicked_count.inc();
        Self::lock_failures(&self.failures).push(BatchTaskFailure::new(index, error));
    }

    /// Returns generic progress counters for this execution state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting.
    #[inline]
    pub(crate) fn progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.task_count))
            .with_active_count(self.active_count.get())
            .with_completed_count(self.completed_count.get())
            .with_succeeded_count(self.succeeded_count.get())
            .with_failed_count(
                self.failed_count
                    .get()
                    .saturating_add(self.panicked_count.get()),
            )
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
    #[inline]
    pub(crate) fn into_outcome(self, elapsed: Duration) -> BatchOutcome<E> {
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchOutcomeBuilder::builder(self.task_count)
            .completed_count(self.completed_count.get())
            .succeeded_count(self.succeeded_count.get())
            .failed_count(self.failed_count.get())
            .panicked_count(self.panicked_count.get())
            .elapsed(elapsed)
            .failures(failures)
            .build()
            .expect("batch execution state should collect consistent counters")
    }

    /// Acquires the failure list lock while tolerating poisoned locks.
    ///
    /// # Parameters
    ///
    /// * `failures` - Failure list mutex to lock.
    ///
    /// # Returns
    ///
    /// A guard for the failure list.
    fn lock_failures(
        failures: &Mutex<Vec<BatchTaskFailure<E>>>,
    ) -> MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
        failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
