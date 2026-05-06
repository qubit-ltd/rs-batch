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

use qubit_progress::model::ProgressCounters;

use crate::BatchProcessResult;

use super::BatchCounter;

/// Shared state collected while a batch processor is running.
pub(crate) struct BatchProcessState {
    /// Atomic processing counters.
    counter: BatchCounter,
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
            counter: BatchCounter::new(item_count),
        }
    }

    /// Records one observed item.
    ///
    /// # Returns
    ///
    /// The observed item count after this item was recorded.
    #[inline]
    pub(crate) fn record_item_observed(&self) -> usize {
        self.counter.record_observed()
    }

    /// Records one successfully processed item.
    #[inline]
    pub(crate) fn record_item_processed(&self) {
        self.counter.record_item_processed();
    }

    /// Records one successfully delegated chunk.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of source items completed by the chunk.
    /// * `processed_count` - Delegate-reported processed item count.
    #[inline]
    pub(crate) fn record_chunk_processed(&self, completed_count: usize, processed_count: usize) {
        self.counter
            .record_chunk_processed(completed_count, processed_count);
    }

    /// Returns the observed item count.
    ///
    /// # Returns
    ///
    /// The number of items observed from the source.
    #[inline]
    pub(crate) fn observed_count(&self) -> usize {
        self.counter.observed_count()
    }

    /// Returns the completed chunk count.
    ///
    /// # Returns
    ///
    /// The number of chunks successfully delegated so far.
    #[inline]
    pub(crate) fn chunk_count(&self) -> usize {
        self.counter.chunk_count()
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
        let processed_count = self.counter.succeeded_count();
        BatchProcessResult::new(
            self.counter.total_count(),
            self.counter.completed_count(),
            processed_count,
            logical_chunk_count(processed_count),
            elapsed,
        )
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
        BatchProcessResult::new(
            self.counter.total_count(),
            self.counter.completed_count(),
            self.counter.succeeded_count(),
            self.counter.chunk_count(),
            elapsed,
        )
    }

    /// Returns generic progress counters for this processing state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting.
    #[inline]
    pub(crate) fn progress_counters(&self) -> ProgressCounters {
        self.counter.progress_counters()
    }

    /// Returns progress counters for in-flight chunk completion reports.
    ///
    /// # Returns
    ///
    /// Counters matching chunked processor running-event semantics.
    #[inline]
    pub(crate) fn running_chunk_progress_counters(&self) -> ProgressCounters {
        self.counter.chunk_running_progress_counters()
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
