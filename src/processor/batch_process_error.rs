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

/// Error returned by built-in consumer-backed batch processors.
///
/// The error variants report mismatches between the declared item count and the
/// number of items yielded by the input source. Each variant carries the partial
/// result accumulated before the mismatch was detected.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BatchProcessError {
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
}

impl BatchProcessError {
    /// Returns the partial result attached to this error.
    ///
    /// # Returns
    ///
    /// A shared reference to the partial batch process result.
    #[inline]
    pub const fn result(&self) -> &BatchProcessResult {
        match self {
            Self::CountShortfall { result, .. } | Self::CountExceeded { result, .. } => result,
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
            Self::CountShortfall { result, .. } | Self::CountExceeded { result, .. } => result,
        }
    }
}
