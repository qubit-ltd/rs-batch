// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_progress::ProgressReportError;
use thiserror::Error;

use super::BatchProcessResult;

/// Error returned by [`crate::ChunkedBatchProcessor`].
///
/// Count-mismatch variants carry the aggregate result accumulated before the
/// mismatch was detected. `ChunkFailed` carries the delegate error plus the
/// aggregate result collected before the failing chunk. `InvalidChunkResult`
/// means the delegate returned `Ok`, but the returned `item_count` or
/// `completed_count` did not match the submitted chunk length.
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
///
/// # Type Parameters
///
/// * `E` - Error type returned by the delegate processor.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChunkedBatchProcessError<E> {
    /// Reporting batch progress failed.
    #[error("batch progress reporting failed")]
    ProgressReport {
        /// Reporter error returned by the configured progress sink.
        #[source]
        source: ProgressReportError,
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
        report_error: Option<ProgressReportError>,
    },

    /// The input source yielded more items than the declared item count.
    #[error(
        "batch item count exceeded: expected {expected}, observed at least {observed_at_least}"
    )]
    CountExceeded {
        /// Declared item count.
        expected: usize,
        /// Lower bound of observed items.
        observed_at_least: usize,
        /// Result accumulated before the excess item was observed.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressReportError>,
    },

    /// The delegate processor failed while processing one chunk.
    #[error(
        "batch chunk {chunk_index} failed at item {start_index} with {chunk_len} items"
    )]
    ChunkFailed {
        /// Zero-based chunk index.
        chunk_index: usize,
        /// Zero-based source item index where this chunk starts.
        start_index: usize,
        /// Number of items submitted in this chunk.
        chunk_len: usize,
        /// Error returned by the delegate processor.
        source: E,
        /// Result accumulated before this chunk failed.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressReportError>,
    },

    /// The delegate returned `Ok` with counters that do not describe the
    /// submitted chunk.
    ///
    /// A successful chunk delegate call must report both `item_count` and
    /// `completed_count` equal to `chunk_len`. A lower `processed_count` is
    /// allowed, but partial chunk completion should be represented by delegate
    /// failure instead of an inconsistent success result.
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
        /// Result accumulated before this invalid chunk result was reported.
        result: BatchProcessResult,
        /// Terminal progress-report error, when reporting the failure failed.
        report_error: Option<ProgressReportError>,
    },
}

impl<E> ChunkedBatchProcessError<E> {
    /// Returns the partial result attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the partial batch process result.
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

    /// Returns the terminal progress-report error attached to this error.
    ///
    /// # Returns
    ///
    /// The report error retained by the primary processing error, if any.
    #[inline]
    pub const fn progress_report_error(&self) -> Option<&ProgressReportError> {
        match self {
            Self::ProgressReport { source, .. } => Some(source),
            Self::CountShortfall { report_error, .. }
            | Self::CountExceeded { report_error, .. }
            | Self::ChunkFailed { report_error, .. }
            | Self::InvalidChunkResult { report_error, .. } => {
                report_error.as_ref()
            }
        }
    }
}
