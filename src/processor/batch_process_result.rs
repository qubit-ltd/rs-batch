/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::{
    fmt,
    time::Duration,
};

/// Structured result produced by a batch processor.
///
/// The result distinguishes completed input items from successfully processed
/// items because some processors can report a success count, such as affected
/// database rows, that differs from the number of input items whose chunk
/// returned.
///
/// # Author
///
/// Haixing Hu
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
    /// Creates a new batch process result.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared item count for the batch.
    /// * `completed_count` - Number of input items that reached a terminal
    ///   processing outcome.
    /// * `processed_count` - Number of items reported as successfully
    ///   processed.
    /// * `chunk_count` - Number of chunks submitted by the processor.
    /// * `elapsed` - Total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// A batch process result with the supplied counters.
    #[inline]
    pub const fn new(
        item_count: usize,
        completed_count: usize,
        processed_count: usize,
        chunk_count: usize,
        elapsed: Duration,
    ) -> Self {
        Self {
            item_count,
            completed_count,
            processed_count,
            chunk_count,
            elapsed,
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
