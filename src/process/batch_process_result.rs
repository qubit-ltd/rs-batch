/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    fmt,
    time::Duration,
};

use crate::BatchProcessResultBuilder;

/// Structured result produced by a batch processor.
///
/// The result distinguishes completed input items from successfully processed
/// items because some processors can report a success count, such as affected
/// database rows, that differs from the number of input items whose chunk
/// returned.
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::BatchProcessResultBuilder;
///
/// let result = BatchProcessResultBuilder::builder(3)
///     .completed_count(3)
///     .processed_count(3)
///     .chunk_count(1)
///     .elapsed(Duration::ZERO)
///     .build()
///     .expect("process result counters should be consistent");
///
/// assert!(result.is_success());
/// assert_eq!(result.item_count(), 3);
/// assert_eq!(result.chunk_count(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchProcessResult {
    /// Declared item count for the batch.
    item_count: usize,
    /// Number of input items whose processing reached a terminal outcome.
    completed_count: usize,
    /// Number of items reported as successfully processed.
    processed_count: usize,
    /// Number of chunks submitted by the processor.
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
    #[inline]
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
    #[inline]
    pub const fn item_count(&self) -> usize {
        self.item_count
    }

    /// Returns how many input items reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The number of completed input items.
    #[inline]
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Returns how many items were reported as successfully processed.
    ///
    /// # Returns
    ///
    /// The processor-reported success count.
    #[inline]
    pub const fn processed_count(&self) -> usize {
        self.processed_count
    }

    /// Returns the number of chunks submitted by the processor.
    ///
    /// # Returns
    ///
    /// The submitted chunk count.
    #[inline]
    pub const fn chunk_count(&self) -> usize {
        self.chunk_count
    }

    /// Returns the total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The elapsed duration for this batch processing attempt.
    #[inline]
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns whether all declared items were processed successfully.
    ///
    /// # Returns
    ///
    /// `true` when every declared item completed and was reported as processed.
    #[inline]
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
