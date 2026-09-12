// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

use super::BatchProcessResult;
use crate::ProgressFailure;

/// Error returned by [`crate::ChunkedBatchProcessor`].
///
/// Count-mismatch variants carry the aggregate result accumulated before the
/// mismatch was detected. `ChunkFailed` carries the delegate error plus the
/// aggregate result collected before the failing chunk. `InvalidChunkResult`
/// means the delegate returned `Ok`, but the returned `item_count` or
/// `completed_count` did not match the submitted chunk length.
///
/// For `ChunkFailed` and `InvalidChunkResult`, the attached result describes
/// only the prefix of chunks that completed successfully before the failing or
/// invalid chunk. The delegate may already have produced external side effects
/// while processing that excluded chunk. Consequently, the aggregate result is
/// not by itself a safe retry boundary; callers must use the delegate's own
/// transaction or idempotency guarantees.
///
/// # Type Parameters
///
/// * `E` - Error type returned by the delegate processor.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::{
///     BatchProcessResult,
///     ChunkedBatchProcessError,
/// };
///
/// let result = BatchProcessResult::builder(4)
///     .completed_count(2)
///     .processed_count(2)
///     .chunk_count(1)
///     .elapsed(Duration::ZERO)
///     .build()
///     .expect("process result counters should be valid");
/// let error: ChunkedBatchProcessError<&'static str> =
///     ChunkedBatchProcessError::ChunkFailed {
///         chunk_index: 1,
///         start_index: 2,
///         chunk_len: 2,
///         source: "insert failed",
///         result,
///         report_error: None,
///     };
///
/// assert_eq!(error.result().processed_count(), 2);
/// ```
#[must_use = "errors describe a rejected operation"]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChunkedBatchProcessError<E> {
    /// Reporting batch progress failed.
    #[error("batch progress reporting failed")]
    ProgressReport {
        /// Reporter error returned by the configured progress sink.
        #[source]
        source: Box<ProgressFailure>,
        /// Result accumulated before reporting failed.
        result: BatchProcessResult,
    },

    /// The input source ended before the declared item count was reached.
    #[error("batch item count shortfall: expected {expected}, actual {actual}")]
    CountShortfall {
        /// Declared item count.
        expected: usize,
        /// Actual number of items observed from the source.
        actual: usize,
        /// Result accumulated before the shortfall was reported.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressFailure>,
    },

    /// The input source yielded more items than the declared item count.
    #[error("batch item count exceeded: expected {expected}, observed at least {observed_at_least}")]
    CountExceeded {
        /// Declared item count.
        expected: usize,
        /// Lower bound of observed items.
        observed_at_least: usize,
        /// Result accumulated before the excess item was observed.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressFailure>,
    },

    /// The delegate processor failed while processing one chunk.
    ///
    /// The attached result ends before this chunk and does not imply that the
    /// failed chunk produced no external side effects.
    #[error("batch chunk {chunk_index} failed at item {start_index} with {chunk_len} items")]
    ChunkFailed {
        /// Zero-based chunk index.
        chunk_index: usize,
        /// Zero-based source item index where this chunk starts.
        start_index: usize,
        /// Number of items submitted in this chunk.
        chunk_len: usize,
        /// Error returned by the delegate processor.
        source: E,
        /// Successful-prefix result accumulated before this failed chunk.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressFailure>,
    },

    /// The delegate returned `Ok` with counters that do not describe the
    /// submitted chunk.
    ///
    /// A successful chunk delegate call must report both `item_count` and
    /// `completed_count` equal to `chunk_len`. A lower `processed_count` is
    /// allowed, but partial chunk completion should be represented by delegate
    /// failure instead of an inconsistent success result. The attached result
    /// ends before this chunk and does not imply that the invalid delegate call
    /// produced no external side effects.
    #[error(
        "batch chunk {chunk_index} returned invalid result at item {start_index}: expected {chunk_len} completed items, got item_count {item_count}, completed_count {completed_count}"
    )]
    InvalidChunkResult {
        /// Zero-based chunk index.
        chunk_index: usize,
        /// Zero-based source item index where this chunk starts.
        start_index: usize,
        /// Number of items submitted in this chunk.
        chunk_len: usize,
        /// Delegate-reported declared item count.
        item_count: usize,
        /// Delegate-reported completed item count.
        completed_count: usize,
        /// Successful-prefix result accumulated before this invalid chunk.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressFailure>,
    },
}

impl<E> ChunkedBatchProcessError<E> {
    /// Returns the partial result attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the partial batch process result.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub const fn result(&self) -> &BatchProcessResult {
        match self {
            Self::ProgressReport { result, .. }
            | Self::CountShortfall { result, .. }
            | Self::CountExceeded { result, .. }
            | Self::ChunkFailed { result, .. }
            | Self::InvalidChunkResult { result, .. } => result,
        }
    }

    /// Returns the primary or secondary progress-report failure.
    ///
    /// # Returns
    ///
    /// `Some` contains the primary progress error or a secondary terminal
    /// error; `None` means reporting succeeded.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub fn progress_report_error(&self) -> Option<&ProgressFailure> {
        match self {
            Self::ProgressReport { source, .. } => Some(source),
            Self::CountShortfall { report_error, .. }
            | Self::CountExceeded { report_error, .. }
            | Self::ChunkFailed { report_error, .. }
            | Self::InvalidChunkResult { report_error, .. } => report_error.as_ref(),
        }
    }

    /// Consumes this error and returns its partial result.
    ///
    /// # Returns
    ///
    /// The partial batch process result.
    #[inline]
    pub fn into_result(self) -> BatchProcessResult {
        match self {
            Self::ProgressReport { result, .. }
            | Self::CountShortfall { result, .. }
            | Self::CountExceeded { result, .. }
            | Self::ChunkFailed { result, .. }
            | Self::InvalidChunkResult { result, .. } => result,
        }
    }
}
