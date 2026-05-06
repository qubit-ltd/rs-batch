/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::{
    BatchCallResult,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
};

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
    let result = BatchCallResult::new(outcome.clone(), vec![Some(10), None]);

    assert_eq!(result.outcome(), &outcome);
    assert_eq!(result.values(), &[Some(10), None]);
    assert_eq!(result.clone().into_values(), vec![Some(10), None]);
    assert_eq!(result.clone().into_outcome(), outcome);
    let (outcome_part, values_part) = result.into_parts();
    assert_eq!(outcome_part.completed_count(), 2);
    assert_eq!(values_part, vec![Some(10), None]);
}
