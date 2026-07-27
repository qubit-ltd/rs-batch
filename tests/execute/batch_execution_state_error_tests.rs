// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`BatchExecutionStateError`](qubit_batch::BatchExecutionStateError).

use qubit_batch::BatchExecutionStateError;

#[test]
fn test_batch_execution_state_error_describes_out_of_range_task_index() {
    let error = BatchExecutionStateError::TaskIndexOutOfRange {
        index: 3,
        task_count: 2,
    };

    assert_eq!(
        error.to_string(),
        "batch task index 3 is outside the declared task count 2"
    );
}
