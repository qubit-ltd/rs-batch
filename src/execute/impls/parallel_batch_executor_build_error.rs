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

/// Error returned when building a [`crate::ParallelBatchExecutor`].
///
/// ```rust
/// use qubit_batch::{
///     ParallelBatchExecutor,
///     ParallelBatchExecutorBuildError,
/// };
///
/// let error = match ParallelBatchExecutor::builder().thread_count(0).build() {
///     Ok(_) => panic!("zero worker count should be rejected"),
///     Err(error) => error,
/// };
///
/// assert_eq!(error, ParallelBatchExecutorBuildError::ZeroThreadCount);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ParallelBatchExecutorBuildError {
    /// The configured worker-thread count is zero.
    #[error("parallel batch executor thread count must be positive")]
    ZeroThreadCount,

    /// The configured progress-report interval is zero.
    #[error("parallel batch executor report interval must be positive")]
    ZeroReportInterval,
}
