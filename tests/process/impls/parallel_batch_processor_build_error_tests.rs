// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchProcessorBuildError`](qubit_batch::ParallelBatchProcessorBuildError).

use qubit_batch::ParallelBatchProcessorBuildError;

#[test]
fn test_parallel_batch_processor_build_error_describes_zero_threads() {
    assert_eq!(
        ParallelBatchProcessorBuildError::ZeroThreadCount.to_string(),
        "parallel batch processor thread count must be positive"
    );
}
