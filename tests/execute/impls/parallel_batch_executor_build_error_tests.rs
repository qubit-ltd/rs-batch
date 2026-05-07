/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ParallelBatchExecutorBuildError`](qubit_batch::ParallelBatchExecutorBuildError).

use qubit_batch::ParallelBatchExecutorBuildError;

#[test]
fn test_parallel_batch_executor_build_error_display_messages() {
    assert_eq!(
        ParallelBatchExecutorBuildError::ZeroThreadCount.to_string(),
        "parallel batch executor thread count must be positive"
    );
}
