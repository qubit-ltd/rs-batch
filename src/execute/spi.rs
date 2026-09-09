// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime integration types for custom parallel batch executors.

mod call_with_executor;

pub use call_with_executor::call_with_executor;

pub use super::parallel_batch_execution_context::ParallelBatchExecutionContext;
pub use super::parallel_batch_execution_coordinator::ParallelBatchExecutionCoordinator;
pub use super::parallel_batch_task::ParallelBatchTask;
