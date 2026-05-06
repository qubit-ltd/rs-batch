/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::Instant;

use crate::state::BatchProcessState;
use qubit_function::{
    BoxConsumer,
    Consumer,
};

use super::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessor,
};

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
        let state = BatchProcessState::new(count);

        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                let result = state.to_direct_result(start.elapsed());
                return Err(BatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: observed_count,
                    result,
                });
            }
            self.consumer.accept(&item);
            state.record_item_processed();
        }

        let result = state.to_direct_result(start.elapsed());
        if state.observed_count() < count {
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else {
            Ok(result)
        }
    }
}
