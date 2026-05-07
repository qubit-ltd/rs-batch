/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::Duration;

use qubit_atomic::AtomicCount;
use qubit_progress::model::ProgressCounters;

use crate::BatchProcessResult;

/// Shared state collected while a batch processor is running.
pub(crate) struct BatchProcessState {
    /// Declared item count.
    item_count: usize,
    /// Number of items observed from the source.
    observed_count: AtomicCount,
    /// Number of items currently being processed.
    active_count: AtomicCount,
    /// Number of input items whose processing completed.
    completed_count: AtomicCount,
    /// Number of items reported as successfully processed.
    processed_count: AtomicCount,
    /// Number of successfully delegated chunks.
    chunk_count: AtomicCount,
}

impl BatchProcessState {
    /// Creates empty processing state for a declared item count.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared number of items in the batch.
    ///
    /// # Returns
    ///
    /// Empty processing state.
    #[inline]
    pub(crate) const fn new(item_count: usize) -> Self {
        Self {
            item_count,
            observed_count: AtomicCount::zero(),
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
            processed_count: AtomicCount::zero(),
            chunk_count: AtomicCount::zero(),
        }
    }

    /// Records one observed item.
    ///
    /// # Returns
    ///
    /// The observed item count after this item was recorded.
    #[inline]
    pub(crate) fn record_item_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records that one item has started processing.
    #[inline]
    pub(crate) fn record_item_started(&self) {
        self.active_count.inc();
    }

    /// Records one successfully processed item.
    #[inline]
    pub(crate) fn record_item_processed(&self) {
        self.active_count.dec();
        self.completed_count.inc();
        self.processed_count.inc();
    }

    /// Records one successfully delegated chunk.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of source items completed by the chunk.
    /// * `processed_count` - Delegate-reported processed item count.
    #[inline]
    pub(crate) fn record_chunk_processed(&self, completed_count: usize, processed_count: usize) {
        self.completed_count.add(completed_count);
        self.processed_count.add(processed_count);
        self.chunk_count.inc();
    }

    /// Returns the observed item count.
    ///
    /// # Returns
    ///
    /// The number of items observed from the source.
    #[inline]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.get()
    }

    /// Returns the completed item count.
    ///
    /// # Returns
    ///
    /// The number of input items completed so far.
    #[inline]
    pub(crate) fn completed_count(&self) -> usize {
        self.completed_count.get()
    }

    /// Returns the completed chunk count.
    ///
    /// # Returns
    ///
    /// The number of chunks successfully delegated so far.
    #[inline]
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunk_count.get()
    }

    /// Converts this state into a direct processor result.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration for the processing attempt.
    ///
    /// # Returns
    ///
    /// A direct processor result containing the current counters.
    #[inline]
    pub(crate) fn to_direct_result(&self, elapsed: Duration) -> BatchProcessResult {
        let processed_count = self.processed_count.get();
        BatchProcessResult::builder(self.item_count)
            .completed_count(self.completed_count.get())
            .processed_count(processed_count)
            .chunk_count(logical_chunk_count(processed_count))
            .elapsed(elapsed)
            .build()
            .expect("direct batch process state should collect consistent counters")
    }

    /// Converts this state into a chunked processor result.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration for the processing attempt.
    ///
    /// # Returns
    ///
    /// A chunked processor result containing the current counters.
    #[inline]
    pub(crate) fn to_chunked_result(&self, elapsed: Duration) -> BatchProcessResult {
        BatchProcessResult::builder(self.item_count)
            .completed_count(self.completed_count.get())
            .processed_count(self.processed_count.get())
            .chunk_count(self.chunk_count.get())
            .elapsed(elapsed)
            .build()
            .expect("chunked batch process state should collect consistent counters")
    }

    /// Returns generic progress counters for this processing state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting.
    #[inline]
    pub(crate) fn progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.item_count))
            .with_active_count(self.active_count.get())
            .with_completed_count(self.completed_count.get())
            .with_succeeded_count(self.processed_count.get())
    }

    /// Returns progress counters for in-flight chunk completion reports.
    ///
    /// # Returns
    ///
    /// Counters matching chunked processor running-event semantics.
    #[inline]
    pub(crate) fn running_chunk_progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.item_count))
            .with_completed_count(self.completed_count.get())
            .with_succeeded_count(self.completed_count.get())
    }
}

/// Converts processed item count to a logical direct-processor chunk count.
///
/// # Parameters
///
/// * `processed_count` - Number of successful consumer calls.
///
/// # Returns
///
/// `1` for non-empty direct processing attempts, or `0` when no item was
/// processed.
#[inline]
const fn logical_chunk_count(processed_count: usize) -> usize {
    if processed_count == 0 { 0 } else { 1 }
}
