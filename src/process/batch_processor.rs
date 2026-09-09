// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::BatchProcessResult;

/// Processes a batch of data items.
///
/// This trait models processors that receive data items directly. A processor
/// may insert records into a database, send them to a remote service, or apply
/// any other batch-level operation chosen by the implementation.
///
/// # When to use a processor
///
/// Use a processor when the operation owns a stateful consumer and should see
/// the input as one logical batch. Typical examples are database batch
/// inserts/updates, remote bulk endpoints, and consumers that need to flush
/// chunks. The processor controls that batching policy and returns
/// [`BatchProcessResult`] counters rather than one outcome record per item.
///
/// In the original Java implementation, DAO batch methods use this shape to
/// split collections into database-safe chunks (for example, below a driver's
/// parameter limit), invoke the DAO operation for each chunk, and aggregate
/// successful input counts. Domain measurements such as affected database rows
/// are not input counts and should be tracked separately by the processor.
/// [`crate::ChunkedBatchProcessor`] is the corresponding Rust abstraction when
/// that chunking is part of the domain contract.
///
/// A result's `processed_count` is the number of successfully processed input
/// items. It must not contain a domain measurement that can exceed the input
/// count. `chunk_count` describes successfully completed chunks at the layer
/// that produced the result; a wrapping chunk processor reports its own chunks
/// rather than summing nested delegate chunk counts.
///
/// Use [`crate::BatchExecutor`] instead when each item is an independent task
/// or callable and callers need per-item failures, panic capture, stable task
/// indexes, or executor-managed parallel scheduling. An executor's
/// `for_each` adapter is intentionally task-oriented; it does not replace a
/// processor whose consumer owns batch state or chunk semantics.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::{
///     BatchProcessResult,
///     BatchProcessResultBuilder,
///     BatchProcessor,
/// };
///
/// struct CountItems;
///
/// impl BatchProcessor<i32> for CountItems {
///     type Error = &'static str;
///
///     fn process_with_count<I>(
///         &mut self,
///         items: I,
///         count: usize,
///     ) -> Result<BatchProcessResult, Self::Error>
///     where
///         I: IntoIterator<Item = i32>,
///     {
///         let processed = items.into_iter().count();
///         BatchProcessResultBuilder::builder(count)
///             .completed_count(processed)
///             .processed_count(processed)
///             .chunk_count(1)
///             .elapsed(Duration::ZERO)
///             .build()
///             .map_err(|_| "invalid process result")
///     }
/// }
///
/// let result = CountItems
///     .process([1, 2, 3])
///     .expect("array length should be exact");
///
/// assert!(result.is_success());
/// ```
///
/// # Type Parameters
///
/// * `Item` - The data item type consumed by this processor.
pub trait BatchProcessor<Item> {
    /// Error returned by this processor.
    type Error;

    /// Processes `items` as one batch using its exact iterator length.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Exact-size item source type.
    ///
    /// # Parameters
    ///
    /// * `items` - Data source for this batch. Its iterator must uphold the
    ///   [`ExactSizeIterator`] contract and report the remaining item count
    ///   exactly.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::process_with_count`] after deriving the
    /// declared count from the iterator length.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the processor rejects the batch. The caller
    /// is responsible for supplying an iterator that upholds its exact-size
    /// contract; implementations may detect a violation, but the trait does
    /// not require a universal runtime check.
    fn process<I>(&mut self, items: I) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
        I::IntoIter: ExactSizeIterator,
    {
        let items = items.into_iter();
        let count = items.len();
        self.process_with_count(items, count)
    }

    /// Processes `items` as one batch with an explicit declared count.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Item source type.
    ///
    /// # Parameters
    ///
    /// * `items` - Data source for this batch.
    /// * `count` - Exact number of items expected from `items`, not an
    ///   estimate, capacity hint, or upper bound. The caller is responsible for
    ///   supplying the correct value.
    ///
    /// # Returns
    ///
    /// Returns a [`BatchProcessResult`] describing the processed batch when the
    /// processor accepts the input.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when this processor cannot process the batch.
    /// Implementations may report a count mismatch when `count` is wrong, but
    /// callers must not rely on every implementation performing that check.
    fn process_with_count<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>;
}
