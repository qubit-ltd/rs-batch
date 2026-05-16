/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use super::BatchProcessResult;

/// Processes a batch of data items.
///
/// This trait models processors that receive data items directly. A processor
/// may insert records into a database, send them to a remote service, or apply
/// any other batch-level operation chosen by the implementation.
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
///
pub trait BatchProcessor<Item> {
    /// Error returned by this processor.
    type Error;

    /// Processes `items` as one batch using its exact iterator length.
    ///
    /// # Parameters
    ///
    /// * `items` - Data source for this batch. Its iterator must report the
    ///   remaining item count exactly.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::process_with_count`] after deriving the
    /// declared count from the iterator length.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the processor rejects the batch or if the
    /// iterator violates its exact length contract while being consumed.
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
    /// # Parameters
    ///
    /// * `items` - Data source for this batch.
    /// * `count` - Declared number of items expected from `items`.
    ///
    /// # Returns
    ///
    /// Returns a [`BatchProcessResult`] describing the processed batch when the
    /// processor accepts the input.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when this processor cannot process the batch.
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>;
}
