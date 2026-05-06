/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use std::time::Duration;

use qubit_batch::{
    BatchOutcome,
    BatchOutcomeBuildError,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
};

#[test]
fn test_batch_outcome_builder_builds_valid_outcome() {
    let failures = vec![
        BatchTaskFailure::new(2, BatchTaskError::panicked("panic")),
        BatchTaskFailure::new(1, BatchTaskError::Failed("failed")),
    ];

    let outcome = BatchOutcomeBuilder::builder(3)
        .completed_count(3)
        .succeeded_count(1)
        .failed_count(1)
        .panicked_count(1)
        .elapsed(Duration::from_millis(5))
        .failures(failures)
        .build()
        .expect("outcome should be valid");

    assert_eq!(outcome.task_count(), 3);
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 1);
    assert_eq!(outcome.failure_count(), 2);
    assert!(!outcome.is_success());
    assert_eq!(outcome.failures()[0].index(), 1);
    assert_eq!(outcome.failures()[1].index(), 2);
}

#[test]
fn test_batch_outcome_builder_validates_before_new() {
    let builder = BatchOutcomeBuilder::<&'static str>::builder(1)
        .completed_count(1)
        .succeeded_count(1)
        .validate()
        .expect("builder should validate consistent counters");

    let outcome = BatchOutcome::new(builder);

    assert!(outcome.is_success());
    assert_eq!(outcome.task_count(), 1);
}

#[test]
fn test_batch_outcome_builder_rejects_invalid_counters() {
    let error = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(3)
        .succeeded_count(3)
        .build()
        .expect_err("completed count should be invalid");

    assert_eq!(
        error,
        BatchOutcomeBuildError::CompletedCountExceeded {
            task_count: 2,
            completed_count: 3,
        }
    );
}
