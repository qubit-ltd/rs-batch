/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`BatchExecutionError`](qubit_batch::BatchExecutionError).

use qubit_batch::{
    BatchExecutionError,
    BatchOutcome,
    BatchOutcomeBuilder,
};

/// Builds a valid batch outcome for error helper tests.
///
/// # Parameters
///
/// * `task_count` - Declared task count.
/// * `completed_count` - Completed task count.
///
/// # Returns
///
/// A valid batch outcome.
fn build_outcome<E>(task_count: usize, completed_count: usize) -> BatchOutcome<E> {
    BatchOutcomeBuilder::builder(task_count)
        .completed_count(completed_count)
        .succeeded_count(completed_count)
        .build()
        .expect("test outcome should satisfy batch outcome invariants")
}

#[test]
fn test_batch_execution_error_shortfall_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        outcome: build_outcome(3, 2),
    };

    assert!(error.is_count_shortfall());
    assert!(!error.is_count_exceeded());
    assert_eq!(error.outcome().completed_count(), 2);

    let outcome = error.into_outcome();

    assert_eq!(outcome.completed_count(), 2);
}

#[test]
fn test_batch_execution_error_shortfall_clone_and_equality() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountShortfall {
        expected: 3,
        actual: 2,
        outcome: build_outcome(3, 2),
    };

    assert_eq!(error.clone(), error);
}

#[test]
fn test_batch_execution_error_exceeded_helpers() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        outcome: build_outcome(2, 2),
    };

    assert!(error.is_count_exceeded());
    assert!(!error.is_count_shortfall());
    assert_eq!(error.outcome().task_count(), 2);
    assert_eq!(error.into_outcome().task_count(), 2);
}

#[test]
fn test_batch_execution_error_exceeded_clone_and_equality() {
    let error: BatchExecutionError<&'static str> = BatchExecutionError::CountExceeded {
        expected: 2,
        observed_at_least: 3,
        outcome: build_outcome(2, 2),
    };

    assert_eq!(error.clone(), error);
}
