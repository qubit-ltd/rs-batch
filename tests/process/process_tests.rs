// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[test]
fn test_process_test_target_builds_valid_result() {
    let result = qubit_batch::BatchProcessResultBuilder::builder(1)
        .completed_count(1)
        .processed_count(1)
        .chunk_count(1)
        .build()
        .expect("consistent process result should build");

    assert!(result.is_success());
}
