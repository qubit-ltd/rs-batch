// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for batch execution defaults.

use qubit_batch::ParallelBatchExecutor;

#[test]
fn test_default_threshold_is_stable() {
    assert_eq!(ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD, 100);
}
