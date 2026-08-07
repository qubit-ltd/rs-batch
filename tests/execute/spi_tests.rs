// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the runtime executor SPI exports.

use qubit_batch::execute::spi::{
    ParallelBatchExecutionContext,
    ParallelBatchExecutionCoordinator,
    ParallelBatchTask,
};

#[test]
fn test_runtime_spi_exports_are_available_under_execute_spi() {
    fn assert_context_type<E>() {}
    fn assert_coordinator_type<T>() {}
    fn assert_task_type<T>() {}

    assert_context_type::<ParallelBatchExecutionContext<&'static str>>();
    assert_coordinator_type::<ParallelBatchExecutionCoordinator>();
    assert_task_type::<ParallelBatchTask<()>>();
}
