// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`BatchProcessResultBuildError`](qubit_batch::BatchProcessResultBuildError).

use qubit_batch::BatchProcessResultBuildError;

#[test]
fn test_batch_process_result_build_error_displays_counter_context() {
    let error = BatchProcessResultBuildError::ProcessedCountExceeded {
        completed_count: 2,
        processed_count: 3,
    };

    assert_eq!(
        error.to_string(),
        "processed item count must not exceed completed item count: completed_count 2, processed_count 3"
    );
}
