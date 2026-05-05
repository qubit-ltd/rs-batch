/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::error::{
    BatchExecutionResult,
    BatchExecutionResultBuildError,
};
use std::time::Duration;

#[test]
fn test_batch_execution_result_build_error_completed_count_exceeded() {
    let error =
        BatchExecutionResult::<&'static str>::try_new(1, 2, 2, 0, 0, Duration::ZERO, Vec::new())
            .expect_err("completed count should be rejected");

    assert_eq!(
        error,
        BatchExecutionResultBuildError::CompletedCountExceeded {
            task_count: 1,
            completed_count: 2,
        }
    );
    assert!(
        error
            .to_string()
            .contains("completed task count must not exceed")
    );
}
