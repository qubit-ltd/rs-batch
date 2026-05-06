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

use crate::{
    NoOpProgressReporter,
    ProgressCounters,
    ProgressPhase,
    ProgressReporter,
    ProgressRun,
};

use super::{
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessError,
};

/// Processes input items by submitting fixed-size chunks to a delegate.
///
/// `ChunkedBatchProcessor` is useful when the caller has a large logical batch
/// but the real target must receive smaller batches, such as SQL batch insert
/// operations with a maximum row count per statement.
///
/// # Type Parameters
///
/// * `P` - Delegate processor receiving each chunk.
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
    /// * `report_interval` - Minimum time between progress callbacks.
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
    /// The minimum time between progress callbacks.
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
    /// match `count`, or when the delegate fails for one chunk.
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let reporter = Arc::clone(&self.reporter);
        let mut progress = ProgressRun::new(reporter.as_ref(), self.report_interval);
        let mut state = ChunkedProcessState::new(count);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            state.progress_counters(),
            Duration::ZERO,
        );
        let capacity = cmp::min(self.chunk_size.get(), count.max(1));
        let mut chunk = Vec::with_capacity(capacity);

        for item in items {
            if state.actual_count == count {
                let result = state.to_result(progress.elapsed());
                progress.report_with_elapsed(
                    ProgressPhase::Failed,
                    state.progress_counters(),
                    result.elapsed(),
                );
                return Err(ChunkedBatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: count + 1,
                    result,
                });
            }
            chunk.push(item);
            state.actual_count += 1;
            if chunk.len() == self.chunk_size.get() {
                self.process_chunk(&mut chunk, &mut state, &mut progress)?;
            }
        }

        if !chunk.is_empty() {
            self.process_chunk(&mut chunk, &mut state, &mut progress)?;
        }

        let result = state.to_result(progress.elapsed());
        if state.actual_count < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                state.progress_counters(),
                result.elapsed(),
            );
            Err(ChunkedBatchProcessError::CountShortfall {
                expected: count,
                actual: state.actual_count,
                result,
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                state.progress_counters(),
                result.elapsed(),
            );
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
        state: &mut ChunkedProcessState,
        progress: &mut ProgressRun<'_>,
    ) -> Result<(), ChunkedBatchProcessError<P::Error>>
    where
        P: BatchProcessor<Item>,
    {
        let chunk_len = chunk.len();
        let start_index = state.actual_count - chunk_len;
        let chunk_index = state.chunk_count;
        let current_chunk = std::mem::take(chunk);
        match self.delegate.process(current_chunk, chunk_len) {
            Ok(chunk_result) => {
                state.completed_count += chunk_len;
                state.processed_count += chunk_result.processed_count();
                state.chunk_count += 1;
                progress.report_running_if_due(state.running_progress_counters());
                Ok(())
            }
            Err(source) => {
                let result = state.to_result(progress.elapsed());
                progress.report_with_elapsed(
                    ProgressPhase::Failed,
                    state.progress_counters(),
                    result.elapsed(),
                );
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

/// Mutable aggregate state for one chunked processing call.
struct ChunkedProcessState {
    /// Declared item count for the logical batch.
    item_count: usize,
    /// Actual number of items observed from the source.
    actual_count: usize,
    /// Number of items whose chunk returned successfully.
    completed_count: usize,
    /// Delegate-reported processed item count.
    processed_count: usize,
    /// Number of chunks successfully submitted.
    chunk_count: usize,
}

impl ChunkedProcessState {
    /// Creates empty aggregate state for a declared item count.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared item count for the logical batch.
    ///
    /// # Returns
    ///
    /// Empty aggregate state.
    const fn new(item_count: usize) -> Self {
        Self {
            item_count,
            actual_count: 0,
            completed_count: 0,
            processed_count: 0,
            chunk_count: 0,
        }
    }

    /// Converts this state into a public process result.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration for the processing attempt.
    ///
    /// # Returns
    ///
    /// A batch process result containing the current counters.
    const fn to_result(&self, elapsed: Duration) -> BatchProcessResult {
        BatchProcessResult::new(
            self.item_count,
            self.completed_count,
            self.processed_count,
            self.chunk_count,
            elapsed,
        )
    }

    /// Returns generic progress counters for this processing state.
    ///
    /// # Returns
    ///
    /// Counters suitable for progress reporting.
    fn progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.item_count))
            .with_completed_count(self.completed_count)
            .with_succeeded_count(self.processed_count)
    }

    /// Returns progress counters for in-flight chunk completion reports.
    ///
    /// # Returns
    ///
    /// Counters matching the previous running-event semantics, where a
    /// successfully accepted chunk marks all of its items as succeeded.
    fn running_progress_counters(&self) -> ProgressCounters {
        ProgressCounters::new(Some(self.item_count))
            .with_completed_count(self.completed_count)
            .with_succeeded_count(self.completed_count)
    }
}
