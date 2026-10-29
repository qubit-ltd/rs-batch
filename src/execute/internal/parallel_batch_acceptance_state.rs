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
    /// Whether the source iterator returned a real terminal `None`.
    source_exhausted: AtomicBool,
}

impl ParallelBatchAcceptanceState {
    /// Creates empty admission state for one declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared number of tasks in the batch.
    ///
    /// # Returns
    ///
    /// A state with zero observed and accepted tasks and no stop or exhaustion
    /// flags set.
    #[inline]
    #[must_use = "use the constructed state"]
    pub(crate) fn new(task_count: usize) -> Self {
        Self {
            task_count,
            observed_count: AtomicUsize::new(0),
            accepted_count: AtomicUsize::new(0),
            stop_accepting: AtomicBool::new(false),
            source_exhausted: AtomicBool::new(false),
        }
    }

    /// Returns the declared task count.
    ///
    /// # Returns
    ///
    /// The task count supplied when this state was constructed.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub(crate) const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns the observed task count.
    ///
    /// # Returns
    ///
    /// The number of source tasks recorded by the scheduler.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.load(Ordering::Acquire)
    }

    /// Returns the accepted task count.
    ///
    /// # Returns
    ///
    /// The number of source tasks accepted for execution.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub(crate) fn accepted_count(&self) -> usize {
        self.accepted_count.load(Ordering::Acquire)
    }

    /// Returns whether source admission has stopped.
    ///
    /// # Returns
    ///
    /// `true` after failure policy has stopped admission; otherwise, `false`.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub(crate) fn should_stop(&self) -> bool {
        self.stop_accepting.load(Ordering::Acquire)
    }

    /// Returns whether the source iterator reached its terminal `None`.
    ///
    /// # Returns
    ///
    /// `true` after the source iterator returned its terminal `None`;
    /// otherwise, `false`.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub(crate) fn source_exhausted(&self) -> bool {
        self.source_exhausted.load(Ordering::Acquire)
    }

    /// Records a real terminal `None` from the source iterator.
    #[inline]
    pub(crate) fn mark_source_exhausted(&self) {
        self.source_exhausted.store(true, Ordering::Release);
    }

    /// Records one observed source task and returns the new total.
    ///
    /// # Returns
    ///
    /// The observed source-task count after recording the task.
    #[inline]
    pub(crate) fn record_observed(&self) -> usize {
        self.observed_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Records an observation when the stop flag is clear at the initial check.
    ///
    /// Because the stop check and counter increment are separate atomic
    /// operations, a concurrent stop may race with this check and the
    /// observation may still be recorded.
    ///
    /// # Returns
    ///
    /// `Some(count)` with the new observed count, or `None` when the initial
    /// stop check finds admission stopped.
    #[inline]
    pub(crate) fn try_record_observed(&self) -> Option<usize> {
        if self.should_stop() {
            None
        } else {
            Some(self.record_observed())
        }
    }

    /// Records one accepted source task and returns the new total.
    ///
    /// # Returns
    ///
    /// The accepted source-task count after recording the task.
    #[inline]
    pub(crate) fn record_accepted(&self) -> usize {
        self.accepted_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Marks source admission as stopped.
    #[inline]
    pub(crate) fn stop(&self) {
        self.stop_accepting.store(true, Ordering::Release);
    }
}
