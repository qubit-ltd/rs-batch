// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for synchronization support.

use qubit_batch::SequentialBatchExecutor;

#[test]
fn synchronization_support_is_internal() {
    assert_eq!(
        SequentialBatchExecutor::DEFAULT_REPORT_INTERVAL.as_secs(),
        5
    );
}
