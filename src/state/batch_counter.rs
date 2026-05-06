/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_atomic::AtomicCount;
use qubit_progress::model::ProgressCounters;

/// Atomic counters shared by batch execution and processing state.
pub(crate) struct BatchCounter {
    /// Declared item or task count.
    total_count: usize,
    /// Number of items or tasks observed from the source.
    observed_count: AtomicCount,
    /// Number of active tasks or work items.
    active_count: AtomicCount,
    /// Number of items or tasks that reached a terminal outcome.
    completed_count: AtomicCount,
    /// Successful task count or processor-reported processed item count.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
    /// Number of completed chunks.
    chunk_count: AtomicCount,
}

impl BatchCounter {
    /// Creates a zeroed counter set for one declared batch.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared item or task count.
    ///
    /// # Returns
    ///
    /// A counter set initialized with zero runtime counters.
    #[inline]
    pub(crate) const fn new(total_count: usize) -> Self {
        Self {
            total_count,
            observed_count: AtomicCount::zero(),
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
            chunk_count: AtomicCount::zero(),
        }
    }

    /// Returns the declared item or task count.
    ///
    /// # Returns
    ///
    /// The expected batch size.
    #[inline]
    pub(crate) const fn total_count(&self) -> usize {
        self.total_count
    }

    /// Returns the observed source count.
    ///
    /// # Returns
    ///
    /// The number of source values consumed far enough to be observed.
    #[inline]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.get()
    }

    /// Returns the active task or work-item count.
    ///
    /// # Returns
    ///
    /// The number of tasks or items currently in flight.
    #[inline]
    pub(crate) fn active_count(&self) -> usize {
        self.active_count.get()
    }

    /// Returns the terminal completion count.
    ///
    /// # Returns
    ///
    /// The number of values that reached a terminal outcome.
    #[inline]
    pub(crate) fn completed_count(&self) -> usize {
        self.completed_count.get()
    }

    /// Returns the success count.
    ///
    /// # Returns
    ///
    /// The successful task count or processed item count.
    #[inline]
    pub(crate) fn succeeded_count(&self) -> usize {
        self.succeeded_count.get()
    }

    /// Returns the failed task count.
    ///
    /// # Returns
    ///
    /// The number of tasks that returned errors.
    #[inline]
    pub(crate) fn failed_count(&self) -> usize {
        self.failed_count.get()
    }

    /// Returns the panicked task count.
    ///
    /// # Returns
    ///
    /// The number of tasks that panicked.
    #[inline]
    pub(crate) fn panicked_count(&self) -> usize {
        self.panicked_count.get()
    }

    /// Returns the combined failure count.
    ///
    /// # Returns
    ///
    /// The sum of failed and panicked task counts.
    #[inline]
    pub(crate) fn failure_count(&self) -> usize {
        self.failed_count() + self.panicked_count()
    }

    /// Returns the completed chunk count.
    ///
    /// # Returns
    ///
    /// The number of chunks completed by a chunked processor.
    #[inline]
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunk_count.get()
    }

    /// Records one observed source value.
    ///
    /// # Returns
    ///
    /// The observed count after this value was recorded.
    #[inline]
    pub(crate) fn record_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records one active work item.
    #[inline]
    pub(crate) fn record_started(&self) {
        self.active_count.inc();
    }

    /// Records one successful task terminal outcome.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this terminal outcome.
    #[inline]
    pub(crate) fn record_task_succeeded(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.succeeded_count.inc();
    }

    /// Records one failed task terminal outcome.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this terminal outcome.
    #[inline]
    pub(crate) fn record_task_failed(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.failed_count.inc();
    }

    /// Records one panicked task terminal outcome.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this terminal outcome.
    #[inline]
    pub(crate) fn record_task_panicked(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.panicked_count.inc();
    }

    /// Records one processed item.
    #[inline]
    pub(crate) fn record_item_processed(&self) {
        self.completed_count.inc();
        self.succeeded_count.inc();
    }

    /// Records one successful chunk.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of source items completed by the chunk.
    /// * `processed_count` - Delegate-reported processed item count.
    #[inline]
    pub(crate) fn record_chunk_processed(&self, completed_count: usize, processed_count: usize) {
        self.completed_count.add(completed_count);
        self.succeeded_count.add(processed_count);
        self.chunk_count.inc();
    }

    /// Builds generic progress counters from current values.
    ///
    /// # Returns
    ///
    /// Progress counters suitable for lifecycle events.
    #[inline]
    pub(crate) fn progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.total_count))
            .with_active_count(self.active_count())
            .with_completed_count(self.completed_count())
            .with_succeeded_count(self.succeeded_count())
            .with_failed_count(self.failure_count())
    }

    /// Builds chunk-running progress counters.
    ///
    /// # Returns
    ///
    /// Progress counters preserving chunked processor running-event semantics.
    #[inline]
    pub(crate) fn chunk_running_progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.total_count))
            .with_completed_count(self.completed_count())
            .with_succeeded_count(self.completed_count())
    }
}
