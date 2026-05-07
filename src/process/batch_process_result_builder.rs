/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::Duration;

use crate::{
    BatchProcessResult,
    BatchProcessResultBuildError,
};

/// Builder carrying validated parts for a [`crate::BatchProcessResult`].
///
/// The builder checks that completed, processed, and chunk counters describe a
/// consistent processing result before creating the result.
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::BatchProcessResultBuilder;
///
/// let result = BatchProcessResultBuilder::builder(3)
///     .completed_count(3)
///     .processed_count(3)
///     .chunk_count(1)
///     .elapsed(Duration::ZERO)
///     .build()
///     .expect("process result counters should be consistent");
///
/// assert!(result.is_success());
/// assert_eq!(result.chunk_count(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchProcessResultBuilder {
    /// Declared item count for the batch.
    pub(crate) item_count: usize,
    /// Number of input items whose processing reached a terminal outcome.
    pub(crate) completed_count: usize,
    /// Number of items reported as successfully processed.
    pub(crate) processed_count: usize,
    /// Number of chunks submitted by the processor.
    pub(crate) chunk_count: usize,
    /// Total monotonic elapsed duration for the batch.
    pub(crate) elapsed: Duration,
}

impl BatchProcessResultBuilder {
    /// Starts building a batch process result.
    ///
    /// # Parameters
    ///
    /// * `item_count` - Declared item count for the batch.
    ///
    /// # Returns
    ///
    /// A builder initialized with zero counters and zero elapsed time.
    #[inline]
    pub const fn builder(item_count: usize) -> Self {
        Self {
            item_count,
            completed_count: 0,
            processed_count: 0,
            chunk_count: 0,
            elapsed: Duration::ZERO,
        }
    }

    /// Sets the number of input items whose processing reached a terminal outcome.
    ///
    /// # Parameters
    ///
    /// * `completed_count` - Number of completed input items.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn completed_count(mut self, completed_count: usize) -> Self {
        self.completed_count = completed_count;
        self
    }

    /// Sets the number of items reported as successfully processed.
    ///
    /// # Parameters
    ///
    /// * `processed_count` - Number of successfully processed items.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn processed_count(mut self, processed_count: usize) -> Self {
        self.processed_count = processed_count;
        self
    }

    /// Sets the number of chunks submitted by the processor.
    ///
    /// # Parameters
    ///
    /// * `chunk_count` - Number of submitted chunks.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn chunk_count(mut self, chunk_count: usize) -> Self {
        self.chunk_count = chunk_count;
        self
    }

    /// Sets the total monotonic elapsed duration.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    #[inline]
    pub const fn elapsed(mut self, elapsed: Duration) -> Self {
        self.elapsed = elapsed;
        self
    }

    /// Validates this builder.
    ///
    /// # Returns
    ///
    /// `Ok(builder)` when all counters are consistent.
    ///
    /// # Errors
    ///
    /// Returns [`BatchProcessResultBuildError`] when the counters are
    /// inconsistent.
    #[inline]
    pub fn validate(self) -> Result<Self, BatchProcessResultBuildError> {
        validate_process_result_invariants(
            self.item_count,
            self.completed_count,
            self.processed_count,
            self.chunk_count,
        )?;
        Ok(self)
    }

    /// Validates this builder and creates a batch process result.
    ///
    /// # Returns
    ///
    /// `Ok(result)` when all counters are consistent.
    ///
    /// # Errors
    ///
    /// Returns [`BatchProcessResultBuildError`] when the counters are
    /// inconsistent.
    #[inline]
    pub fn build(self) -> Result<BatchProcessResult, BatchProcessResultBuildError> {
        self.validate().map(BatchProcessResult::new)
    }
}

/// Validates all counters for a batch process result.
fn validate_process_result_invariants(
    item_count: usize,
    completed_count: usize,
    processed_count: usize,
    chunk_count: usize,
) -> Result<(), BatchProcessResultBuildError> {
    if completed_count > item_count {
        return Err(BatchProcessResultBuildError::CompletedCountExceeded {
            item_count,
            completed_count,
        });
    }
    if processed_count > completed_count {
        return Err(BatchProcessResultBuildError::ProcessedCountExceeded {
            completed_count,
            processed_count,
        });
    }
    if completed_count > 0 && chunk_count == 0 {
        return Err(BatchProcessResultBuildError::MissingChunkForCompletedItems {
            completed_count,
        });
    }
    if chunk_count > completed_count {
        return Err(BatchProcessResultBuildError::ChunkCountExceeded {
            completed_count,
            chunk_count,
        });
    }
    Ok(())
}
