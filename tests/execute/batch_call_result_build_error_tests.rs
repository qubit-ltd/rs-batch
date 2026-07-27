// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`BatchCallResultBuildError`](qubit_batch::BatchCallResultBuildError).

use qubit_batch::BatchCallResultBuildError;

#[test]
fn test_batch_call_result_build_error_displays_value_mapping_context() {
    let count_error = BatchCallResultBuildError::SucceededValueCountMismatch {
        succeeded_count: 2,
        value_count: 1,
    };
    let index_error =
        BatchCallResultBuildError::FailureValuePresent { index: 3 };

    assert_eq!(
        count_error.to_string(),
        "successful callable value count must equal succeeded task count: succeeded_count 2, value_count 1"
    );
    assert_eq!(
        index_error.to_string(),
        "failed or panicked callable at index 3 must not contain a value"
    );
}
