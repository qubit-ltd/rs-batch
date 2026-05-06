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
    num::NonZeroUsize,
    sync::Arc,
    thread,
    time::Instant,
};

use qubit_function::{
    ArcConsumer,
    Consumer,
};

use crate::process::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessState,
    BatchProcessor,
};
use crate::utils::run_scoped_parallel;

/// Processes batch items in parallel on scoped standard threads.
///
/// The processor stores the supplied consumer as an [`ArcConsumer`] so every
/// worker can share it safely. Worker threads are scoped to each
/// [`BatchProcessor::process`] call, therefore input items may borrow data from
/// the caller as long as they are [`Send`].
///
/// # Type Parameters
///
/// * `Item` - Item type consumed by the stored consumer.
pub struct ParallelBatchProcessor<Item> {
    /// Consumer shared by all scoped workers.
    consumer: ArcConsumer<Item>,
    /// Fixed worker-thread count used by each processing call.
    thread_count: NonZeroUsize,
}

impl<Item> ParallelBatchProcessor<Item> {
    /// Creates a parallel consumer-backed batch processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Thread-safe consumer invoked once for each accepted item.
    ///
    /// # Returns
    ///
    /// A processor storing `consumer` as an [`ArcConsumer`] and using
    /// [`Self::default_thread_count`] workers.
    #[inline]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + Send + Sync + 'static,
    {
        Self {
            consumer: consumer.into_arc(),
            thread_count: NonZeroUsize::new(Self::default_thread_count())
                .expect("default parallel processor thread count should be non-zero"),
        }
    }

    /// Returns the default worker-thread count.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[inline]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }

    /// Returns a copy configured with a fixed worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Non-zero number of scoped worker threads.
    ///
    /// # Returns
    ///
    /// This processor configured to use `thread_count` workers per call.
    #[inline]
    pub const fn with_thread_count(mut self, thread_count: NonZeroUsize) -> Self {
        self.thread_count = thread_count;
        self
    }

    /// Returns the configured worker-thread count.
    ///
    /// # Returns
    ///
    /// The maximum number of scoped worker threads used for one batch.
    #[inline]
    pub const fn thread_count(&self) -> usize {
        self.thread_count.get()
    }

    /// Returns the stored consumer.
    ///
    /// # Returns
    ///
    /// A shared reference to the arc-backed consumer.
    #[inline]
    pub const fn consumer(&self) -> &ArcConsumer<Item> {
        &self.consumer
    }

    /// Consumes this processor and returns the stored consumer.
    ///
    /// # Returns
    ///
    /// The arc-backed consumer used by this processor.
    #[inline]
    pub fn into_consumer(self) -> ArcConsumer<Item> {
        self.consumer
    }
}

impl<Item> BatchProcessor<Item> for ParallelBatchProcessor<Item>
where
    Item: Send,
{
    type Error = BatchProcessError;

    /// Processes items on fixed-width scoped standard threads.
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
    /// Propagates any panic raised by the stored consumer from a worker thread.
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let start = Instant::now();
        let state = Arc::new(BatchProcessState::new(count));

        if count > 0 {
            self.process_non_empty(items, count, Arc::clone(&state));
        } else if items.into_iter().next().is_some() {
            state.record_item_observed();
        }

        let result = state.to_direct_result(start.elapsed());
        if state.observed_count() < count {
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else if state.observed_count() > count {
            Err(BatchProcessError::CountExceeded {
                expected: count,
                observed_at_least: state.observed_count(),
                result,
            })
        } else {
            Ok(result)
        }
    }
}

impl<Item> ParallelBatchProcessor<Item>
where
    Item: Send,
{
    /// Processes a non-empty declared batch through scoped workers.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared item count.
    /// * `state` - Shared processing state updated by producer and workers.
    ///
    /// # Panics
    ///
    /// Propagates any worker panic raised while invoking the stored consumer.
    fn process_non_empty<I>(&self, items: I, count: usize, state: Arc<BatchProcessState>)
    where
        I: IntoIterator<Item = Item>,
    {
        let worker_count = self.thread_count.get().min(count);
        let observer_state = Arc::clone(&state);
        let worker_state = Arc::clone(&state);
        let consumer = self.consumer.clone();
        run_scoped_parallel(
            items,
            count,
            worker_count,
            move || observer_state.record_item_observed(),
            move |_index, item| {
                consumer.accept(&item);
                worker_state.record_item_processed();
            },
        );
    }
}
