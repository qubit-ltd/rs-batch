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
    Mutex, MutexGuard,
    atomic::{AtomicUsize, Ordering},
};

use std::time::Duration;

use crate::{BatchOutcome, BatchOutcomeBuilder, BatchTaskError, BatchTaskFailure};

/// Shared result counters and failure storage for a running parallel batch.
pub(crate) struct ParallelBatchResultState<E> {
    /// Number of successful tasks.
    succeeded_count: AtomicUsize,
    /// Number of failed tasks.
    failed_count: AtomicUsize,
    /// Number of panicked tasks.
    panicked_count: AtomicUsize,
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
            succeeded_count: AtomicUsize::new(0),
            failed_count: AtomicUsize::new(0),
            panicked_count: AtomicUsize::new(0),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Records one successful task.
    pub(crate) fn record_task_succeeded(&self) {
        self.succeeded_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    pub(crate) fn record_task_failed(&self, index: usize, error: E) {
        self.failed_count.fetch_add(1, Ordering::AcqRel);
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
        self.panicked_count.fetch_add(1, Ordering::AcqRel);
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
            .succeeded_count(self.succeeded_count.load(Ordering::Acquire))
            .failed_count(self.failed_count.load(Ordering::Acquire))
            .panicked_count(self.panicked_count.load(Ordering::Acquire))
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
