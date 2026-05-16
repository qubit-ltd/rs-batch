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

/// Error returned when building a [`crate::ParallelBatchProcessor`].
///
/// ```rust
/// use qubit_batch::{
///     ParallelBatchProcessor,
///     ParallelBatchProcessorBuildError,
/// };
///
/// let error = match ParallelBatchProcessor::builder(|_item: &i32| {})
///     .thread_count(0)
///     .build()
/// {
///     Ok(_) => panic!("zero worker count should be rejected"),
///     Err(error) => error,
/// };
///
/// assert_eq!(error, ParallelBatchProcessorBuildError::ZeroThreadCount);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ParallelBatchProcessorBuildError {
    /// The configured worker-thread count is zero.
    #[error("parallel batch processor thread count must be positive")]
    ZeroThreadCount,
}
