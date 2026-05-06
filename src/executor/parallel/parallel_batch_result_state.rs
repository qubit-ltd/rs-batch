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

use crate::{
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
};

/// Shared result counters and failure storage for a running parallel batch.
pub(crate) struct ParallelBatchResultState<E> {
    /// Number of successful tasks.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
    /// Detailed task failure list.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> ParallelBatchResultState<E> {
    /// Creates fresh result state for one batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters and no recorded failures.
    pub(crate) fn new() -> Self {
        Self {
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Records one successful task.
    pub(crate) fn record_task_succeeded(&self) {
        self.succeeded_count.inc();
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    pub(crate) fn record_task_failed(&self, index: usize, error: E) {
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
    pub(crate) fn record_task_panicked(&self, index: usize, error: BatchTaskError<E>) {
        self.panicked_count.inc();
        Self::lock_failures(&self.failures).push(BatchTaskFailure::new(index, error));
    }

    /// Builds a structured batch result from collected state.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared batch task count.
    /// * `completed_count` - Number of tasks completed by workers.
    /// * `elapsed` - Total elapsed wall-clock time.
    ///
    /// # Returns
    ///
    /// A structured batch execution result.
    pub(crate) fn into_outcome(
        self,
        task_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) -> BatchOutcome<E> {
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchOutcomeBuilder::builder(task_count)
            .completed_count(completed_count)
            .succeeded_count(self.succeeded_count.get())
            .failed_count(self.failed_count.get())
            .panicked_count(self.panicked_count.get())
            .elapsed(elapsed)
            .failures(failures)
            .build()
            .expect("parallel batch executor should collect consistent counters")
    }

    fn lock_failures(
        failures: &Mutex<Vec<BatchTaskFailure<E>>>,
    ) -> MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
        failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
