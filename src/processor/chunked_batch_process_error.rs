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
    error::Error,
    fmt,
};

use super::BatchProcessResult;

/// Error returned by [`crate::ChunkedBatchProcessor`].
///
/// # Type Parameters
///
/// * `E` - Error type returned by the delegate processor.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkedBatchProcessError<E> {
    /// The input source ended before the declared item count was reached.
    CountShortfall {
        /// Declared item count.
        expected: usize,
        /// Actual number of items observed from the source.
        actual: usize,
        /// Result accumulated before the shortfall was reported.
        result: BatchProcessResult,
    },

    /// The input source yielded more items than the declared item count.
    CountExceeded {
        /// Declared item count.
        expected: usize,
        /// Lower bound of observed items.
        observed_at_least: usize,
        /// Result accumulated before the excess item was observed.
        result: BatchProcessResult,
    },

    /// The delegate processor failed while processing one chunk.
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

impl<E> fmt::Display for ChunkedBatchProcessError<E> {
    /// Formats this chunked batch process error.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the error text.
    ///
    /// # Returns
    ///
    /// The formatter result.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountShortfall {
                expected, actual, ..
            } => write!(
                f,
                "batch item count shortfall: expected {expected}, actual {actual}"
            ),
            Self::CountExceeded {
                expected,
                observed_at_least,
                ..
            } => write!(
                f,
                "batch item count exceeded: expected {expected}, observed at least {observed_at_least}"
            ),
            Self::ChunkFailed {
                chunk_index,
                start_index,
                chunk_len,
                ..
            } => write!(
                f,
                "batch chunk {chunk_index} failed at item {start_index} with {chunk_len} items"
            ),
        }
    }
}

impl<E> Error for ChunkedBatchProcessError<E>
where
    E: Error + 'static,
{
    /// Returns the delegate processor error when this is a chunk failure.
    ///
    /// # Returns
    ///
    /// `Some(source)` for [`Self::ChunkFailed`], or `None` for source-count
    /// mismatch errors.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ChunkFailed { source, .. } => Some(source),
            Self::CountShortfall { .. } | Self::CountExceeded { .. } => None,
        }
    }
}
