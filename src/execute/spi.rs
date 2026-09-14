// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime integration types for custom parallel batch executors.
//!
//! A scheduler must consume the source supplied by
//! [`ParallelBatchExecutionCoordinator::execute_with_source`]. The older
//! scheduler-owned admission entry point is deliberately absent:
//!
//! ```compile_fail
//! use std::convert::Infallible;
//! use std::sync::Arc;
//! use std::time::Duration;
//! use qubit_batch::TaskFailurePolicy;
//! use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
//! use qubit_progress::NoopReporter;
//!
//! let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
//! let _ = coordinator.execute::<_, (), Infallible, _>(
//!     std::iter::empty::<fn() -> Result<(), ()>>(),
//!     0,
//!     TaskFailurePolicy::Continue,
//!     |_tasks, _context| Ok(()),
//! );
//! ```

mod call_with_executor;

pub use call_with_executor::call_with_executor;

pub use super::parallel_batch_execution_context::ParallelBatchExecutionContext;
pub use super::parallel_batch_execution_coordinator::ParallelBatchExecutionCoordinator;
pub use super::parallel_batch_source::ParallelBatchSource;
pub use super::parallel_batch_task::ParallelBatchTask;
