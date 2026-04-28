/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use super::BatchProcessResult;

/// Processes a declared batch of data items.
///
/// This trait models processors that receive data items directly. A processor
/// may insert records into a database, send them to a remote service, or apply
/// any other batch-level operation chosen by the implementation.
///
/// # Type Parameters
///
/// * `Item` - The data item type consumed by this processor.
///
/// # Author
///
/// Haixing Hu
pub trait BatchProcessor<Item> {
    /// Error returned by this processor.
    type Error;

    /// Processes `items` as one declared batch.
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
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>;
}
