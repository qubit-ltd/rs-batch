/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use thiserror::Error;

use super::BatchProcessResult;

/// Error returned by [`crate::ChunkedBatchProcessor`].
///
/// Count-mismatch variants carry the aggregate result accumulated before the
/// mismatch was detected. `ChunkFailed` carries the delegate error plus the
/// aggregate result collected before the failing chunk.
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::{
///     BatchProcessResult,
///     ChunkedBatchProcessError,
/// };
///
/// let result = BatchProcessResult::new(4, 2, 2, 1, Duration::ZERO);
/// let error: ChunkedBatchProcessError<&'static str> =
///     ChunkedBatchProcessError::ChunkFailed {
///         chunk_index: 1,
///         start_index: 2,
///         chunk_len: 2,
///         source: "insert failed",
///         result,
///     };
///
/// assert_eq!(error.result().processed_count(), 2);
/// ```
///
/// # Type Parameters
///
/// * `E` - Error type returned by the delegate processor.
///
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ChunkedBatchProcessError<E> {
    /// The input source ended before the declared item count was reached.
    #[error("batch item count shortfall: expected {expected}, actual {actual}")]
    CountShortfall {
        /// Declared item count.
        expected: usize,
        /// Actual number of items observed from the source.
        actual: usize,
        /// Result accumulated before the shortfall was reported.
        result: BatchProcessResult,
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
    },

    /// The delegate processor failed while processing one chunk.
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
        /// Result accumulated before this chunk failed.
        result: BatchProcessResult,
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
            Self::CountShortfall { result, .. }
            | Self::CountExceeded { result, .. }
            | Self::ChunkFailed { result, .. } => result,
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
            Self::CountShortfall { result, .. }
            | Self::CountExceeded { result, .. }
            | Self::ChunkFailed { result, .. } => result,
        }
    }
}
