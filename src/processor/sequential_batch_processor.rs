/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::{Duration, Instant};

use qubit_function::{BoxConsumer, Consumer};

use super::{BatchProcessError, BatchProcessResult, BatchProcessor};

/// Processes batch items sequentially by invoking a [`Consumer`] per item.
///
/// The processor stores the supplied consumer as a [`BoxConsumer`] and invokes it
/// on the caller thread in input order. Consumer panics are not caught; they
/// propagate to the caller and no [`BatchProcessResult`] is produced.
///
/// # Type Parameters
///
/// * `Item` - Item type consumed by the stored consumer.
pub struct SequentialBatchProcessor<Item> {
    /// Consumer called once for each accepted item.
    consumer: BoxConsumer<Item>,
}

impl<Item> SequentialBatchProcessor<Item> {
    /// Creates a sequential consumer-backed batch processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Consumer invoked once for each input item.
    ///
    /// # Returns
    ///
    /// A processor storing `consumer` as a [`BoxConsumer`].
    #[inline]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + 'static,
    {
        Self {
            consumer: consumer.into_box(),
        }
    }

    /// Returns the stored consumer.
    ///
    /// # Returns
    ///
    /// A shared reference to the boxed consumer.
    #[inline]
    pub const fn consumer(&self) -> &BoxConsumer<Item> {
        &self.consumer
    }

    /// Consumes this processor and returns the stored consumer.
    ///
    /// # Returns
    ///
    /// The boxed consumer used by this processor.
    #[inline]
    pub fn into_consumer(self) -> BoxConsumer<Item> {
        self.consumer
    }
}

impl<Item> BatchProcessor<Item> for SequentialBatchProcessor<Item> {
    type Error = BatchProcessError;

    /// Processes items sequentially on the caller thread.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared number of items expected from `items`.
    ///
    /// # Returns
    ///
    /// A result with completed and processed counts equal to the number of
    /// consumer calls when the input source yields exactly `count` items.
    ///
    /// # Errors
    ///
    /// Returns [`BatchProcessError::CountShortfall`] when the source ends before
    /// `count`, or [`BatchProcessError::CountExceeded`] when the source yields an
    /// extra item. Extra items are observed but not passed to the consumer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stored consumer.
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let start = Instant::now();
        let mut processed_count = 0usize;

        for item in items {
            if processed_count == count {
                let result = process_result(count, processed_count, start.elapsed());
                return Err(BatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: count + 1,
                    result,
                });
            }
            self.consumer.accept(&item);
            processed_count += 1;
        }

        let result = process_result(count, processed_count, start.elapsed());
        if processed_count < count {
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: processed_count,
                result,
            })
        } else {
            Ok(result)
        }
    }
}

/// Builds a process result for a direct consumer-backed processor.
///
/// # Parameters
///
/// * `item_count` - Declared item count.
/// * `processed_count` - Number of successful consumer calls.
/// * `elapsed` - Total elapsed duration for this processing attempt.
///
/// # Returns
///
/// A process result where direct processors count the whole non-empty attempt as
/// one logical chunk.
#[inline]
const fn process_result(
    item_count: usize,
    processed_count: usize,
    elapsed: Duration,
) -> BatchProcessResult {
    BatchProcessResult::new(
        item_count,
        processed_count,
        processed_count,
        logical_chunk_count(processed_count),
        elapsed,
    )
}

/// Converts processed item count to a logical chunk count.
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
