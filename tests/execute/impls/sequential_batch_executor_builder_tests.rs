// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`SequentialBatchExecutorBuilder`](qubit_batch::SequentialBatchExecutorBuilder).

use std::time::Duration;

use qubit_batch::SequentialBatchExecutor;

#[test]
fn test_sequential_batch_executor_builder_applies_report_interval() {
    let executor = SequentialBatchExecutor::builder()
        .report_interval(Duration::ZERO)
        .no_reporter()
        .build();

    assert_eq!(executor.report_interval(), Duration::ZERO);
}
