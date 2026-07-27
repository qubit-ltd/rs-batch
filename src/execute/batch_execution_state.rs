// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use qubit_atomic::AtomicCount;
use qubit_function::Runnable;
use qubit_progress::model::ProgressCounter;

use crate::{
    BatchExecutionStateError, BatchOutcome, BatchOutcomeBuilder, BatchTaskError, BatchTaskFailure,
    execute::panic_payload_to_error,
};

/// Metric id used for task progress counters.
pub(crate) const EXECUTION_PROGRESS_METRIC_ID: &str = "tasks";

/// Metric display name used for task progress counters.
pub(crate) const EXECUTION_PROGRESS_METRIC_NAME: &str = "Tasks";

/// Shared state collected while a batch executor is running.
pub struct BatchExecutionState<E> {
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
    pub const fn new(task_count: usize) -> Self {
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

    /// Executes one indexed task and records its terminal outcome.
    ///
    /// Task-returned errors and captured panics are stored in this state and do
    /// not become this method's error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based index of `task` within the declared batch.
    /// * `task` - Runnable task executed synchronously by this call.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the task's terminal outcome was recorded, or
    /// [`BatchExecutionStateError::TaskIndexOutOfRange`] before executing an
    /// out-of-range task.
    #[allow(deprecated)]
    pub fn execute_task<T>(&self, index: usize, mut task: T) -> Result<(), BatchExecutionStateError>
    where
        T: Runnable<E>,
    {
        if index >= self.task_count {
            return Err(BatchExecutionStateError::TaskIndexOutOfRange {
                index,
                task_count: self.task_count,
            });
        }
        self.record_task_started();
        match catch_unwind(AssertUnwindSafe(|| task.run())) {
            Ok(Ok(())) => self.record_task_succeeded(),
            Ok(Err(error)) => self.record_task_failed(index, error),
            Err(payload) => {
                self.record_task_panicked(index, panic_payload_to_error(payload.as_ref()))
            }
        }
        Ok(())
    }

    /// Records one observed task.
    ///
    /// # Returns
    ///
    /// The observed task count after this task was recorded.
    #[inline]
    pub fn record_task_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records that one task has started.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_started(&self) {
        self.active_count.inc();
    }

    /// Records one successful task completion.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_succeeded(&self) {
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
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_failed(&self, index: usize, error: E) {
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
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_panicked(&self, index: usize, error: BatchTaskError<E>) {
        self.active_count.dec();
        self.completed_count.inc();
        self.panicked_count.inc();
        Self::lock_failures(&self.failures).push(BatchTaskFailure::new(index, error));
    }

    /// Returns the number of task errors and captured task panics.
    ///
    /// # Returns
    ///
    /// The total terminal task failure count recorded so far.
    #[inline]
    pub fn failure_count(&self) -> usize {
        self.failed_count
            .get()
            .saturating_add(self.panicked_count.get())
    }

    /// Returns progress counters for this execution state.
    ///
    /// # Returns
    ///
    /// A single task counter suitable for progress reporting.
    #[inline]
    pub fn progress_counters(&self) -> Vec<ProgressCounter> {
        vec![
            ProgressCounter::new(EXECUTION_PROGRESS_METRIC_ID)
                .total(self.task_count as u64)
                .active(self.active_count.get() as u64)
                .completed(self.completed_count.get() as u64)
                .succeeded(self.succeeded_count.get() as u64)
                .failed(self.failure_count() as u64),
        ]
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
    ///
    /// # Panics
    ///
    /// Panics if callers used the low-level recording methods to create
    /// counters or failure details that violate [`BatchOutcome`] invariants.
    #[inline]
    pub fn into_outcome(self, elapsed: Duration) -> BatchOutcome<E> {
        self.try_into_outcome(elapsed)
            .expect("batch execution state should collect consistent counters")
    }

    /// Consumes this state and validates the resulting batch outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// A validated final or partial outcome, or a build error if low-level
    /// recording calls created inconsistent counters.
    #[inline]
    pub fn try_into_outcome(
        self,
        elapsed: Duration,
    ) -> Result<BatchOutcome<E>, crate::BatchOutcomeBuildError> {
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
