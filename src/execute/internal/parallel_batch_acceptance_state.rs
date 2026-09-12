// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::sync::AtomicBool;
use crate::sync::AtomicUsize;
use crate::sync::Ordering;

/// Atomic source-admission state shared by scheduler and worker threads.
pub(crate) struct ParallelBatchAcceptanceState {
    /// Declared task count.
    task_count: usize,
    /// Number of source tasks observed by the scheduler.
    observed_count: AtomicUsize,
    /// Number of source tasks accepted for execution.
    accepted_count: AtomicUsize,
    /// Whether new tasks are rejected after a failure-policy stop.
    stop_accepting: AtomicBool,
}

impl ParallelBatchAcceptanceState {
    /// Creates empty admission state for one declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared number of tasks in the batch.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) fn new(task_count: usize) -> Self {
        Self {
            task_count,
            observed_count: AtomicUsize::new(0),
            accepted_count: AtomicUsize::new(0),
            stop_accepting: AtomicBool::new(false),
        }
    }

    /// Returns the declared task count.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns the observed task count.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.load(Ordering::Acquire)
    }

    /// Returns the accepted task count.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn accepted_count(&self) -> usize {
        self.accepted_count.load(Ordering::Acquire)
    }

    /// Returns whether source admission has stopped.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn should_stop(&self) -> bool {
        self.stop_accepting.load(Ordering::Acquire)
    }

    /// Records one observed source task and returns the new total.
    #[inline(always)]
    pub(crate) fn record_observed(&self) -> usize {
        self.observed_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Records an observation unless failure policy has already stopped
    /// admission.
    #[inline]
    pub(crate) fn try_record_observed(&self) -> Option<usize> {
        if self.should_stop() {
            None
        } else {
            Some(self.record_observed())
        }
    }

    /// Records one accepted source task and returns the new total.
    #[inline(always)]
    pub(crate) fn record_accepted(&self) -> usize {
        self.accepted_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Marks source admission as stopped.
    #[inline(always)]
    pub(crate) fn stop(&self) {
        self.stop_accepting.store(true, Ordering::Release);
    }
}
