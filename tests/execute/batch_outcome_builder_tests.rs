// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use qubit_batch::BatchOutcomeBuildError;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;

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
fn test_batch_outcome_builder_builds_after_validation() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(1)
        .completed_count(1)
        .succeeded_count(1)
        .build()
        .expect("builder should validate consistent counters before building");

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

#[test]
fn test_batch_outcome_builder_rejects_duplicate_failure_indexes() {
    let error = BatchOutcomeBuilder::builder(2)
        .completed_count(2)
        .failed_count(1)
        .panicked_count(1)
        .failures(vec![
            BatchTaskFailure::new(0, BatchTaskError::Failed("failed")),
            BatchTaskFailure::new(0, BatchTaskError::panicked("panic")),
        ])
        .build()
        .expect_err("duplicate failure indexes should be rejected");

    assert_eq!(
        error,
        BatchOutcomeBuildError::DuplicateFailureIndex { index: 0 }
    );
}

/// Range violations take priority over duplicate indexes regardless of order.
#[test]
fn test_out_of_range_precedes_duplicate_index() {
    let error = BatchOutcomeBuilder::builder(3)
        .completed_count(3)
        .failed_count(3)
        .failures(vec![
            BatchTaskFailure::new(0, BatchTaskError::Failed("first")),
            BatchTaskFailure::new(0, BatchTaskError::Failed("duplicate")),
            BatchTaskFailure::new(4, BatchTaskError::Failed("out of range")),
        ])
        .build()
        .expect_err("out-of-range failure must be rejected first");
    assert_eq!(
        error,
        BatchOutcomeBuildError::FailureIndexOutOfRange {
            index: 4,
            task_count: 3
        }
    );
}

/// Unsorted invalid input reports the smallest duplicate after range checks.
#[test]
fn test_smallest_duplicate_index_is_reported() {
    let error = BatchOutcomeBuilder::builder(4)
        .completed_count(4)
        .failed_count(4)
        .failures(
            [3, 3, 1, 1]
                .into_iter()
                .map(|index| BatchTaskFailure::new(index, BatchTaskError::Failed("duplicate")))
                .collect(),
        )
        .build()
        .expect_err("repeated failure indexes must be rejected");
    assert_eq!(
        error,
        BatchOutcomeBuildError::DuplicateFailureIndex { index: 1 }
    );
}

#[test]
fn test_batch_outcome_rejects_failure_detail_mismatches() {
    let failure = BatchTaskFailure::new(3, BatchTaskError::Failed("failed"));
    assert!(matches!(
        BatchOutcomeBuilder::builder(2)
            .completed_count(1)
            .failed_count(1)
            .failures(vec![failure])
            .build(),
        Err(BatchOutcomeBuildError::FailureIndexOutOfRange { .. })
    ));

    let failure: BatchTaskFailure<&'static str> =
        BatchTaskFailure::new(0, BatchTaskError::panicked("panic"));
    assert!(matches!(
        BatchOutcomeBuilder::builder(2)
            .completed_count(1)
            .failed_count(1)
            .failures(vec![failure])
            .build(),
        Err(BatchOutcomeBuildError::FailureVariantCountMismatch { .. })
    ));

    assert!(matches!(
        BatchOutcomeBuilder::<&'static str>::builder(2)
            .completed_count(1)
            .failed_count(usize::MAX)
            .panicked_count(1)
            .build(),
        Err(BatchOutcomeBuildError::FailureCountOverflow { .. })
    ));

    assert!(matches!(
        BatchOutcomeBuilder::<&'static str>::builder(usize::MAX)
            .succeeded_count(usize::MAX)
            .failed_count(1)
            .build(),
        Err(BatchOutcomeBuildError::TerminalCountOverflow { .. })
    ));

    assert!(matches!(
        BatchOutcomeBuilder::<&'static str>::builder(2)
            .completed_count(1)
            .succeeded_count(1)
            .failed_count(1)
            .build(),
        Err(BatchOutcomeBuildError::TerminalCountMismatch { .. })
    ));

    assert!(matches!(
        BatchOutcomeBuilder::<&'static str>::builder(2)
            .completed_count(1)
            .failed_count(1)
            .build(),
        Err(BatchOutcomeBuildError::FailureDetailCountMismatch { .. })
    ));
}
