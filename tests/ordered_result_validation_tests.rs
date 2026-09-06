// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for ordered sparse callable-result validation.

use qubit_batch::BatchCallOutput;
use qubit_batch::BatchCallResult;
use qubit_batch::BatchCallResultBuildError;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;

#[test]
fn sparse_successes_and_failures_keep_original_indexes() {
    let outcome = BatchOutcomeBuilder::builder(1_000_000)
        .completed_count(4)
        .succeeded_count(2)
        .failed_count(2)
        .failures(vec![
            BatchTaskFailure::new(999_999, BatchTaskError::Failed("last")),
            BatchTaskFailure::new(1, BatchTaskError::Failed("first")),
        ])
        .build()
        .expect("outcome should be valid");
    let result = BatchCallResult::try_new(
        outcome,
        vec![BatchCallOutput::new(0, "a"), BatchCallOutput::new(900_000, "b")],
    )
    .expect("sparse outputs should be valid");
    assert_eq!(result.outputs()[1].index(), 900_000);
    assert_eq!(result.outcome().failures()[0].index(), 1);
}

#[test]
fn overlap_error_precedes_success_count_mismatch() {
    let outcome = BatchOutcomeBuilder::builder(3)
        .completed_count(3)
        .succeeded_count(1)
        .failed_count(2)
        .failures(vec![
            BatchTaskFailure::new(1, BatchTaskError::Failed("a")),
            BatchTaskFailure::new(2, BatchTaskError::Failed("b")),
        ])
        .build()
        .expect("outcome should be valid");
    let error = BatchCallResult::try_new(outcome, vec![BatchCallOutput::new(0, ()), BatchCallOutput::new(2, ())])
        .expect_err("failure output overlap should be rejected");
    assert_eq!(error, BatchCallResultBuildError::FailureOutputPresent { index: 2 });
}
