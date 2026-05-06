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
    panic::resume_unwind,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use qubit_function::{ArcConsumer, Consumer};

use super::{BatchProcessError, BatchProcessResult, BatchProcessor};

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
        let mut actual_count = 0usize;
        let processed_count = Arc::new(AtomicUsize::new(0));

        if count > 0 {
            self.process_non_empty(
                items,
                count,
                &mut actual_count,
                Arc::clone(&processed_count),
            );
        } else {
            actual_count = observe_zero_count_input(items);
        }

        let processed_count = processed_count.load(Ordering::Acquire);
        let result = process_result(count, processed_count, start.elapsed());
        if actual_count < count {
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: actual_count,
                result,
            })
        } else if actual_count > count {
            Err(BatchProcessError::CountExceeded {
                expected: count,
                observed_at_least: actual_count,
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
    /// * `actual_count` - Mutable counter updated with the observed item count.
    /// * `processed_count` - Shared successful consumer-call counter.
    ///
    /// # Panics
    ///
    /// Propagates any worker panic raised while invoking the stored consumer.
    fn process_non_empty<I>(
        &self,
        items: I,
        count: usize,
        actual_count: &mut usize,
        processed_count: Arc<AtomicUsize>,
    ) where
        I: IntoIterator<Item = Item>,
    {
        let worker_count = self.thread_count.get().min(count);
        thread::scope(|scope| {
            let (task_sender, task_receiver) = mpsc::channel();
            let task_receiver = Arc::new(Mutex::new(task_receiver));
            let mut worker_handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let worker_receiver = Arc::clone(&task_receiver);
                let worker_consumer = self.consumer.clone();
                let worker_processed_count = Arc::clone(&processed_count);
                worker_handles.push(scope.spawn(move || {
                    run_parallel_processor_worker(
                        worker_receiver,
                        worker_consumer,
                        worker_processed_count,
                    );
                }));
            }

            for item in items {
                if *actual_count == count {
                    *actual_count += 1;
                    break;
                }
                *actual_count += 1;
                task_sender
                    .send(item)
                    .expect("parallel batch processor workers should accept items");
            }
            drop(task_sender);

            for handle in worker_handles {
                if let Err(payload) = handle.join() {
                    resume_unwind(payload);
                }
            }
        });
    }
}

/// Runs one processor worker until the task channel closes.
///
/// # Parameters
///
/// * `task_receiver` - Shared receiver protected because standard receivers are
///   not `Sync`.
/// * `consumer` - Consumer invoked for every received item.
/// * `processed_count` - Shared counter incremented after each consumer call.
///
/// # Panics
///
/// Propagates any panic raised by `consumer`.
fn run_parallel_processor_worker<Item>(
    task_receiver: Arc<Mutex<mpsc::Receiver<Item>>>,
    consumer: ArcConsumer<Item>,
    processed_count: Arc<AtomicUsize>,
) {
    loop {
        let received = task_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        let Ok(item) = received else {
            break;
        };
        consumer.accept(&item);
        processed_count.fetch_add(1, Ordering::AcqRel);
    }
}

/// Observes whether a zero-count input source contains any extra item.
///
/// # Parameters
///
/// * `items` - Item source for a batch declared with zero items.
///
/// # Returns
///
/// `0` when the source is empty, or `1` when at least one excess item exists.
fn observe_zero_count_input<Item, I>(items: I) -> usize
where
    I: IntoIterator<Item = Item>,
{
    usize::from(items.into_iter().next().is_some())
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
