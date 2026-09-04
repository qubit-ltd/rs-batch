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
    #[inline]
    pub(crate) fn new(task_count: usize) -> Self {
        Self {
            task_count,
            observed_count: AtomicUsize::new(0),
            accepted_count: AtomicUsize::new(0),
            stop_accepting: AtomicBool::new(false),
        }
    }

    /// Records one observed source task and returns the new total.
    #[inline]
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
    #[inline]
    pub(crate) fn record_accepted(&self) -> usize {
        self.accepted_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Returns the declared task count.
    #[inline]
    pub(crate) const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns the observed task count.
    #[inline]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.load(Ordering::Acquire)
    }

    /// Returns the accepted task count.
    #[inline]
    pub(crate) fn accepted_count(&self) -> usize {
        self.accepted_count.load(Ordering::Acquire)
    }

    /// Marks source admission as stopped.
    #[inline]
    pub(crate) fn stop(&self) {
        self.stop_accepting.store(true, Ordering::Release);
    }

    /// Returns whether source admission has stopped.
    #[inline]
    pub(crate) fn should_stop(&self) -> bool {
        self.stop_accepting.load(Ordering::Acquire)
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use std::sync::Arc;

    use loom::model;
    use loom::thread;

    use super::ParallelBatchAcceptanceState;

    #[test]
    fn loom_failure_stop_is_observable_and_never_reopens_admission() {
        model(|| {
            let state = Arc::new(ParallelBatchAcceptanceState::new(2));
            let worker_state = Arc::clone(&state);
            let handle = thread::spawn(move || {
                worker_state.stop();
                worker_state.try_record_observed()
            });
            assert!(handle.join().expect("loom worker should join").is_none());
            assert!(state.should_stop());
        });
    }

    #[test]
    fn loom_concurrent_admission_preserves_each_atomic_counter() {
        model(|| {
            let state = Arc::new(ParallelBatchAcceptanceState::new(2));
            let first = Arc::clone(&state);
            let second = Arc::clone(&state);
            let first_handle = thread::spawn(move || {
                first.record_observed();
                first.record_accepted();
            });
            let second_handle = thread::spawn(move || {
                second.record_observed();
                second.record_accepted();
            });
            first_handle.join().expect("first loom worker should join");
            second_handle.join().expect("second loom worker should join");
            assert_eq!(state.observed_count(), 2);
            assert_eq!(state.accepted_count(), 2);
        });
    }
}
