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

use qubit_progress::model::ProgressCounters;

use crate::{
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
};

use super::BatchCounter;

/// Shared state collected while a batch executor is running.
pub(crate) struct BatchExecutionState<E> {
    /// Atomic execution counters.
    counter: BatchCounter,
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
            counter: BatchCounter::new(task_count),
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
        self.counter.record_observed()
    }

    /// Records that one task has started.
    #[inline]
    pub(crate) fn record_task_started(&self) {
        self.counter.record_started();
    }

    /// Records one successful task completion.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[inline]
    pub(crate) fn record_task_succeeded(&self) {
        self.counter.record_task_succeeded();
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
        self.counter.record_task_failed();
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
        self.counter.record_task_panicked();
        Self::lock_failures(&self.failures).push(BatchTaskFailure::new(index, error));
    }

    /// Returns generic progress counters for this execution state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting.
    #[inline]
    pub(crate) fn progress_counters(&self) -> ProgressCounters {
        self.counter.progress_counters()
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
        BatchOutcomeBuilder::builder(self.counter.total_count())
            .completed_count(self.counter.completed_count())
            .succeeded_count(self.counter.succeeded_count())
            .failed_count(self.counter.failed_count())
            .panicked_count(self.counter.panicked_count())
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
