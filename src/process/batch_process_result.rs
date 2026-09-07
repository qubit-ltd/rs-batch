// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;
use std::time::Duration;

use crate::BatchProcessResultBuilder;

/// Structured result produced by a batch processor.
///
/// The result distinguishes completed input items from successfully processed
/// input items. Its counters satisfy
/// `processed_count <= completed_count <= item_count`. Measurements that are
/// not input counts, such as affected database rows, must be tracked
/// separately. `chunk_count` counts successfully completed chunks at the
/// processing layer represented by this result. It does not count failed
/// submission attempts or automatically include chunks used inside a nested
/// delegate.
///
/// # Examples
///
/// ```rust
/// use std::cell::Cell;
/// use std::time::Duration;
///
/// use qubit_batch::BatchProcessResultBuilder;
///
/// // These two successful inputs changed three database rows each. Affected
/// // rows are a domain measurement, so keep them outside the process result.
/// let affected_rows = Cell::new(6);
/// let result = BatchProcessResultBuilder::builder(2)
///     .completed_count(2)
///     .processed_count(2)
///     .chunk_count(1)
///     .elapsed(Duration::ZERO)
///     .build()
///     .expect("process result counters should be consistent");
///
/// assert!(result.is_success());
/// assert_eq!(result.processed_count(), 2);
/// assert_eq!(affected_rows.get(), 6);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "inspect the returned value"]
pub struct BatchProcessResult {
    /// Declared item count for the batch.
    item_count: usize,
    /// Number of input items whose processing reached a terminal outcome.
    completed_count: usize,
    /// Number of input items processed successfully.
    processed_count: usize,
    /// Number of chunks completed successfully at this processing layer.
    chunk_count: usize,
    /// Total monotonic elapsed duration.
    elapsed: Duration,
}

impl BatchProcessResult {
    /// Starts building a batch process result.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared item count for the batch.
    ///
    /// # Returns
    ///
    /// A result builder initialized with zero counters and zero elapsed time.
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
    pub const fn builder(item_count: usize) -> BatchProcessResultBuilder {
        BatchProcessResultBuilder::builder(item_count)
    }

    /// Creates a new batch process result from a validated builder.
    ///
    /// # Parameters
    ///
    /// * `builder` - Validated process result builder carrying all result
    ///   fields.
    ///
    /// # Returns
    ///
    /// A fully populated batch process result.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) const fn new(builder: BatchProcessResultBuilder) -> Self {
        Self {
            item_count: builder.item_count,
            completed_count: builder.completed_count,
            processed_count: builder.processed_count,
            chunk_count: builder.chunk_count,
            elapsed: builder.elapsed,
        }
    }

    /// Returns the declared item count.
    ///
    /// # Returns
    ///
    /// The expected number of input items.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn item_count(&self) -> usize {
        self.item_count
    }

    /// Returns how many input items reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The number of completed input items.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Returns how many input items were processed successfully.
    ///
    /// # Returns
    ///
    /// The successful input count. Domain measurements such as affected rows
    /// are tracked separately.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn processed_count(&self) -> usize {
        self.processed_count
    }

    /// Returns the number of chunks completed successfully at this layer.
    ///
    /// # Returns
    ///
    /// The successful chunk count. Failed attempts and nested delegate chunks
    /// are not included in an outer processor's result.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn chunk_count(&self) -> usize {
        self.chunk_count
    }

    /// Returns the total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The elapsed duration for this batch processing attempt.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns whether all declared items were processed successfully.
    ///
    /// # Returns
    ///
    /// `true` when every declared item completed and was reported as processed.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn is_success(&self) -> bool {
        self.completed_count == self.item_count && self.processed_count == self.item_count
    }
}

impl fmt::Display for BatchProcessResult {
    /// Formats a concise summary of this batch process result.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the summary text.
    ///
    /// # Returns
    ///
    /// The formatter result.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "processed {}/{} items in {} chunks ({:?})",
            self.processed_count, self.item_count, self.chunk_count, self.elapsed
        )
    }
}
