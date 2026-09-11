// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_batch::BatchCallOutput;
use qubit_batch::BatchCallResult;
use qubit_batch::BatchCallResultBuildError;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;

#[test]
fn test_overlap_error_precedes_output_count_error() {
    let outcome = BatchOutcomeBuilder::builder(3)
        .completed_count(3)
        .succeeded_count(1)
        .failed_count(2)
        .failures(vec![
            BatchTaskFailure::new(2, BatchTaskError::Failed(())),
            BatchTaskFailure::new(1, BatchTaskError::Failed(())),
        ])
        .build()
        .expect("outcome should be valid");

    let error = BatchCallResult::try_new(
        outcome,
        vec![BatchCallOutput::new(0, 10), BatchCallOutput::new(1, 20)],
    )
    .expect_err("a failed task must not have a callable output");

    assert_eq!(
        error,
        BatchCallResultBuildError::FailureOutputPresent { index: 1 }
    );
}

#[test]
fn test_large_disjoint_outputs_and_failures_are_accepted() {
    const TASK_COUNT: usize = 100_000;
    const SUCCESS_COUNT: usize = TASK_COUNT / 2;

    let failures = (1..TASK_COUNT)
        .step_by(2)
        .map(|index| BatchTaskFailure::new(index, BatchTaskError::Failed(())))
        .collect();
    let outcome = BatchOutcomeBuilder::builder(TASK_COUNT)
        .completed_count(TASK_COUNT)
        .succeeded_count(SUCCESS_COUNT)
        .failed_count(SUCCESS_COUNT)
        .failures(failures)
        .build()
        .expect("large outcome should be valid");
    let outputs = (0..TASK_COUNT)
        .step_by(2)
        .map(|index| BatchCallOutput::new(index, index))
        .collect();

    let result = BatchCallResult::try_new(outcome, outputs)
        .expect("disjoint successful outputs and failures should be accepted");

    assert_eq!(result.outputs().len(), SUCCESS_COUNT);
    assert_eq!(
        result.outputs().first().map(BatchCallOutput::index),
        Some(0)
    );
    assert_eq!(
        result.outputs().last().map(BatchCallOutput::index),
        Some(TASK_COUNT - 2)
    );
}

#[test]
fn test_sparse_successes_and_failures_keep_original_indexes() {
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
        vec![
            BatchCallOutput::new(0, "a"),
            BatchCallOutput::new(900_000, "b"),
        ],
    )
    .expect("sparse outputs should be valid");
    assert_eq!(result.outputs()[1].index(), 900_000);
    assert_eq!(result.outcome().failures()[0].index(), 1);
}
