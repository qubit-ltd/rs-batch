// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_batch::BatchCallResult;
use qubit_batch::BatchCallResultBuildError;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;

#[test]
fn test_batch_call_result_accessors_and_parts() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .panicked_count(1)
        .failures(vec![BatchTaskFailure::new(
            1,
            BatchTaskError::panicked("panic"),
        )])
        .build()
        .expect("outcome should be valid");
    let result =
        BatchCallResult::try_new(outcome.clone(), vec![Some(10), None])
            .expect("value slots should match the declared task count");

    assert_eq!(result.outcome(), &outcome);
    assert_eq!(result.values(), &[Some(10), None]);
    assert_eq!(result.clone().into_values(), vec![Some(10), None]);
    assert_eq!(result.clone().into_outcome(), outcome);
    let (outcome_part, values_part) = result.into_parts();
    assert_eq!(outcome_part.completed_count(), 2);
    assert_eq!(values_part, vec![Some(10), None]);
}

#[test]
fn test_batch_call_result_rejects_mismatched_value_count() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(1)
        .completed_count(1)
        .succeeded_count(1)
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::<usize, &'static str>::try_new(outcome, Vec::new()),
        Err(BatchCallResultBuildError::ValueCountMismatch {
            task_count: 1,
            value_count: 0,
        })
    );
}

#[test]
fn test_batch_call_result_rejects_value_at_failed_callable_index() {
    let outcome = BatchOutcomeBuilder::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .failed_count(1)
        .failures(vec![BatchTaskFailure::new(
            1,
            BatchTaskError::Failed("failed callable"),
        )])
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::try_new(outcome, vec![Some(10), Some(20)]),
        Err(BatchCallResultBuildError::FailureValuePresent { index: 1 })
    );
}

#[test]
fn test_batch_call_result_rejects_mismatched_success_value_count() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(2)
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::try_new(outcome, vec![Some(10), None]),
        Err(BatchCallResultBuildError::SucceededValueCountMismatch {
            succeeded_count: 2,
            value_count: 1,
        })
    );
}
