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
    BatchOutcome,
    BatchTaskError,
    BatchTaskFailure,
};
use std::time::Duration;

#[test]
fn test_batch_call_result_accessors_and_parts() {
    let outcome = BatchOutcome::<&'static str>::try_new(
        2,
        2,
        1,
        0,
        1,
        Duration::ZERO,
        vec![BatchTaskFailure::new(1, BatchTaskError::panicked("panic"))],
    )
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
