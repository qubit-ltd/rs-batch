// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use qubit_atomic::AtomicCount;
use qubit_progress::MetricDelta;
use qubit_progress::MetricError;
use qubit_progress::MetricHandle;

use crate::BatchProcessResult;

/// Metric id used for item progress counters.
pub(crate) const PROCESS_PROGRESS_METRIC_ID: &str = "items";

/// Metric display name used for item progress counters.
pub(crate) const PROCESS_PROGRESS_METRIC_NAME: &str = "Items";

/// Shared state collected while a batch processor is running.
pub(crate) struct BatchProcessState {
    /// Declared item count.
    item_count: usize,
    /// Number of items observed from the source.
    observed_count: AtomicCount,
    /// Progress-owned lifecycle state for item counts.
    metric: MetricHandle,
    /// Number of successfully delegated chunks.
    chunk_count: AtomicCount,
}

impl BatchProcessState {
    /// Creates empty processing state for a declared item count.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared number of items in the batch.
    /// * `metric` - Lifecycle metric recording completed and successful items.
    ///
    /// # Returns
    ///
    /// Empty processing state.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) fn new(item_count: usize, metric: MetricHandle) -> Self {
        Self {
            item_count,
            observed_count: AtomicCount::zero(),
            metric,
            chunk_count: AtomicCount::zero(),
        }
    }

    /// Returns the observed item count.
    ///
    /// # Returns
    ///
    /// The number of items observed from the source.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn observed_count(&self) -> usize {
        self.observed_count.get()
    }

    /// Returns the completed item count.
    ///
    /// # Returns
    ///
    /// The number of input items completed so far.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn completed_count(&self) -> usize {
        self.metric.snapshot().completed() as usize
    }

    /// Returns the completed chunk count.
    ///
    /// # Returns
    ///
    /// The number of chunks successfully delegated so far.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunk_count.get()
    }

    /// Records one observed item.
    ///
    /// # Returns
    ///
    /// The observed item count after this item was recorded.
    #[inline(always)]
    pub(crate) fn record_item_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records that one item has started processing.
    #[inline(always)]
    pub(crate) fn record_item_started(&self) -> Result<(), MetricError> {
        self.metric.start(1)
    }

    /// Records one successfully processed item.
    #[inline(always)]
    pub(crate) fn record_item_processed(&self) -> Result<(), MetricError> {
        self.metric.succeed(1)
    }

    /// Records one successfully delegated chunk.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of source items completed by the chunk.
    /// * `processed_count` - Delegate-reported processed item count.
    #[inline]
    pub(crate) fn record_chunk_processed(
        &self,
        completed_count: usize,
        processed_count: usize,
    ) -> Result<(), MetricError> {
        let completed_count = completed_count as u64;
        let processed_count = processed_count as u64;
        self.metric.apply_delta(
            MetricDelta::new()
                .started(completed_count)
                .succeeded(processed_count)
                .unclassified(completed_count - processed_count),
        )?;
        self.chunk_count.inc();
        Ok(())
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
        let snapshot = self.metric.snapshot();
        let processed_count = snapshot.succeeded() as usize;
        BatchProcessResult::builder(self.item_count)
            .completed_count(snapshot.completed() as usize)
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
        let snapshot = self.metric.snapshot();
        BatchProcessResult::builder(self.item_count)
            .completed_count(snapshot.completed() as usize)
            .processed_count(snapshot.succeeded() as usize)
            .chunk_count(self.chunk_count.get())
            .elapsed(elapsed)
            .build()
            .expect("chunked batch process state should collect consistent counters")
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
