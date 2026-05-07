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

/// Error returned when constructing a batch process result with invalid counters.
///
/// ```rust
/// use qubit_batch::{
///     BatchProcessResultBuildError,
///     BatchProcessResultBuilder,
/// };
///
/// let error = BatchProcessResultBuilder::builder(1)
///     .completed_count(2)
///     .processed_count(2)
///     .chunk_count(1)
///     .build()
///     .expect_err("completed count should not exceed declared item count");
///
/// assert!(matches!(
///     error,
///     BatchProcessResultBuildError::CompletedCountExceeded { .. }
/// ));
/// ```
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchProcessResultBuildError {
    /// The completed item count is greater than the declared item count.
    #[error(
        "completed item count must not exceed declared item count: item_count {item_count}, completed_count {completed_count}"
    )]
    CompletedCountExceeded {
        /// Declared item count.
        item_count: usize,
        /// Number of completed items.
        completed_count: usize,
    },

    /// The processed item count is greater than the completed item count.
    #[error(
        "processed item count must not exceed completed item count: completed_count {completed_count}, processed_count {processed_count}"
    )]
    ProcessedCountExceeded {
        /// Number of completed items.
        completed_count: usize,
        /// Number of successfully processed items.
        processed_count: usize,
    },

    /// Completed items require at least one submitted chunk.
    #[error("chunk count must be positive when items completed: completed_count {completed_count}")]
    MissingChunkForCompletedItems {
        /// Number of completed items.
        completed_count: usize,
    },

    /// The submitted chunk count is greater than the completed item count.
    #[error(
        "chunk count must not exceed completed item count: completed_count {completed_count}, chunk_count {chunk_count}"
    )]
    ChunkCountExceeded {
        /// Number of completed items.
        completed_count: usize,
        /// Number of submitted chunks.
        chunk_count: usize,
    },
}
