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
    cmp,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use qubit_progress::{
    Progress,
    reporter::{
        NoOpProgressReporter,
        ProgressReporter,
    },
};

use crate::process::{
    BatchProcessResult,
    BatchProcessState,
    BatchProcessor,
    ChunkedBatchProcessError,
};

/// Processes input items by submitting fixed-size chunks to a delegate.
///
/// `ChunkedBatchProcessor` is useful when the caller has a large logical batch
/// but the real target must receive smaller batches, such as SQL batch insert
/// operations with a maximum row count per statement.
///
/// The delegate must return a result whose `item_count` and `completed_count`
/// match the submitted chunk length whenever it returns `Ok`. The delegate may
/// still report a lower `processed_count`, such as when a database reports
/// fewer affected rows than submitted rows. If the delegate cannot reach a
/// terminal outcome for every item in the chunk, it should return `Err`;
/// inconsistent `Ok` results are returned as
/// [`ChunkedBatchProcessError::InvalidChunkResult`].
///
/// # Type Parameters
///
/// * `P` - Delegate processor receiving each chunk.
///
/// ```rust
/// use std::{
///     num::NonZeroUsize,
///     time::Duration,
/// };
///
/// use qubit_batch::{
///     BatchProcessResult,
///     BatchProcessResultBuilder,
///     BatchProcessor,
///     ChunkedBatchProcessor,
/// };
///
/// struct InsertChunk;
///
/// impl BatchProcessor<i32> for InsertChunk {
///     type Error = &'static str;
///
///     fn process_with_count<I>(
///         &mut self,
///         rows: I,
///         count: usize,
///     ) -> Result<BatchProcessResult, Self::Error>
///     where
///         I: IntoIterator<Item = i32>,
///     {
///         let processed = rows.into_iter().count();
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
/// let mut processor = ChunkedBatchProcessor::new(
///     InsertChunk,
///     NonZeroUsize::new(2).expect("chunk size should be non-zero"),
/// );
///
/// let result = processor
///     .process([1, 2, 3, 4, 5])
///     .expect("array length should be exact");
///
/// assert_eq!(result.completed_count(), 5);
/// assert_eq!(result.chunk_count(), 3);
/// ```
///
pub struct ChunkedBatchProcessor<P> {
    /// Delegate processor receiving each chunk.
    delegate: P,
    /// Maximum number of items submitted to the delegate at once.
    chunk_size: NonZeroUsize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl<P> ChunkedBatchProcessor<P> {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Creates a chunked batch processor.
    ///
    /// # Parameters
    ///
    /// * `delegate` - Processor receiving each chunk.
    /// * `chunk_size` - Maximum number of items submitted in one chunk.
    ///
    /// # Returns
    ///
    /// A chunked processor using [`NoOpProgressReporter`].
    ///
    /// # Type Constraints
    ///
    /// This constructor only stores `delegate`; it intentionally does not
    /// require `P: BatchProcessor<Item>` because the item type is not part of
    /// construction. That bound is enforced when this wrapper is used as a
    /// [`BatchProcessor<Item>`], such as when calling [`BatchProcessor::process`].
    /// Therefore, a value can be constructed with any delegate type, but it can
    /// only process items for item types that the delegate actually supports.
    #[inline]
    pub fn new(delegate: P, chunk_size: NonZeroUsize) -> Self {
        Self {
            delegate,
            chunk_size,
            report_interval: Self::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter<R>(self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.with_reporter_arc(Arc::new(reporter))
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter_arc(self, reporter: Arc<dyn ProgressReporter>) -> Self {
        Self { reporter, ..self }
    }

    /// Returns a copy configured with the supplied progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every completed-chunk
    ///   progress point.
    ///
    /// # Returns
    ///
    /// This processor configured with `report_interval`.
    #[inline]
    pub fn with_report_interval(self, report_interval: Duration) -> Self {
        Self {
            report_interval,
            ..self
        }
    }

    /// Returns the configured chunk size.
    ///
    /// # Returns
    ///
    /// The maximum number of items submitted to the delegate at once.
    #[inline]
    pub const fn chunk_size(&self) -> NonZeroUsize {
        self.chunk_size
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between due-based running progress callbacks.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the configured progress reporter.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.reporter
    }

    /// Returns a shared reference to the delegate processor.
    ///
    /// # Returns
    ///
    /// The wrapped delegate processor.
    #[inline]
    pub const fn delegate(&self) -> &P {
        &self.delegate
    }

    /// Returns a mutable reference to the delegate processor.
    ///
    /// # Returns
    ///
    /// The wrapped delegate processor.
    #[inline]
    pub fn delegate_mut(&mut self) -> &mut P {
        &mut self.delegate
    }

    /// Consumes this wrapper and returns the delegate processor.
    ///
    /// # Returns
    ///
    /// The wrapped delegate processor.
    #[inline]
    pub fn into_delegate(self) -> P {
        self.delegate
    }
}

impl<Item, P> BatchProcessor<Item> for ChunkedBatchProcessor<P>
where
    P: BatchProcessor<Item>,
{
    type Error = ChunkedBatchProcessError<P::Error>;

    /// Processes items by delegating fixed-size chunks.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the logical batch.
    /// * `count` - Declared number of items expected from `items`.
    ///
    /// # Returns
    ///
    /// A result aggregating all successfully delegated chunks.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkedBatchProcessError`] when the source count does not
    /// match `count`, when the delegate fails for one chunk, or when a delegate
    /// `Ok` result does not describe the submitted chunk.
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let reporter = Arc::clone(&self.reporter);
        let mut progress = Progress::new(reporter.as_ref(), self.report_interval);
        let state = BatchProcessState::new(count);
        progress.report_started(state.progress_counters());
        let capacity = cmp::min(self.chunk_size.get(), count.max(1));
        let mut chunk = Vec::with_capacity(capacity);

        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                if !chunk.is_empty() {
                    self.process_chunk(&mut chunk, &state, &mut progress)?;
                }
                let failed = progress.report_failed(state.progress_counters());
                let result = state.to_chunked_result(failed.elapsed());
                return Err(ChunkedBatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: observed_count,
                    result,
                });
            }
            chunk.push(item);
            if chunk.len() == self.chunk_size.get() {
                self.process_chunk(&mut chunk, &state, &mut progress)?;
            }
        }

        if !chunk.is_empty() {
            self.process_chunk(&mut chunk, &state, &mut progress)?;
        }

        if state.observed_count() < count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.to_chunked_result(failed.elapsed());
            Err(ChunkedBatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else {
            let finished = progress.report_finished(state.progress_counters());
            let result = state.to_chunked_result(finished.elapsed());
            Ok(result)
        }
    }
}

impl<P> ChunkedBatchProcessor<P> {
    /// Submits one collected chunk to the delegate and updates aggregate state.
    ///
    /// # Parameters
    ///
    /// * `chunk` - Buffered items waiting to be submitted.
    /// * `state` - Aggregate counters updated after successful delegation.
    /// * `progress` - Progress run used for lifecycle and periodic callbacks.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the delegate accepts the chunk.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkedBatchProcessError::ChunkFailed`] when the delegate
    /// returns an error.
    fn process_chunk<Item>(
        &mut self,
        chunk: &mut Vec<Item>,
        state: &BatchProcessState,
        progress: &mut Progress<'_>,
    ) -> Result<(), ChunkedBatchProcessError<P::Error>>
    where
        P: BatchProcessor<Item>,
    {
        let chunk_len = chunk.len();
        let start_index = state.completed_count();
        let chunk_index = state.chunk_count();
        let current_chunk = std::mem::take(chunk);
        match self.delegate.process_with_count(current_chunk, chunk_len) {
            Ok(chunk_result) => {
                if chunk_result.item_count() != chunk_len
                    || chunk_result.completed_count() != chunk_len
                {
                    let failed = progress.report_failed(state.progress_counters());
                    let result = state.to_chunked_result(failed.elapsed());
                    return Err(ChunkedBatchProcessError::InvalidChunkResult {
                        chunk_index,
                        start_index,
                        chunk_len,
                        item_count: chunk_result.item_count(),
                        completed_count: chunk_result.completed_count(),
                        result,
                    });
                }
                state.record_chunk_processed(chunk_len, chunk_result.processed_count());
                let _ = progress.report_running_if_due(state.running_chunk_progress_counters());
                Ok(())
            }
            Err(source) => {
                let failed = progress.report_failed(state.progress_counters());
                let result = state.to_chunked_result(failed.elapsed());
                Err(ChunkedBatchProcessError::ChunkFailed {
                    chunk_index,
                    start_index,
                    chunk_len,
                    source,
                    result,
                })
            }
        }
    }
}
