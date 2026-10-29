// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::BatchOutcomeBuildError;
use crate::BatchTaskError;
use crate::BatchTaskFailure;

/// Validates all counters and failure details for a batch outcome.
///
/// # Type Parameters
///
/// * `E` - Task-specific error type stored in failure records.
///
/// # Parameters
///
/// * `task_count` - Declared task count for the batch.
/// * `completed_count` - Number of tasks that reached a terminal outcome.
/// * `succeeded_count` - Number of tasks that completed successfully.
/// * `failed_count` - Number of tasks that returned a business error.
/// * `panicked_count` - Number of tasks whose bodies panicked.
/// * `failures` - Mutable failure detail slice sorted and checked in place.
///
/// # Returns
///
/// `Ok(())` when aggregate counters and failure details are consistent.
///
/// # Errors
///
/// Returns [`BatchOutcomeBuildError`] when counters overflow, disagree, or
/// failure details do not match the aggregate failure counts.
pub(in crate::execute) fn validate_outcome_invariants<E>(
    task_count: usize,
    completed_count: usize,
    succeeded_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &mut [BatchTaskFailure<E>],
) -> Result<(), BatchOutcomeBuildError> {
    let failure_count =
        failed_count
            .checked_add(panicked_count)
            .ok_or(BatchOutcomeBuildError::FailureCountOverflow {
                failed_count,
                panicked_count,
            })?;
    let terminal_count =
        succeeded_count
            .checked_add(failure_count)
            .ok_or(BatchOutcomeBuildError::TerminalCountOverflow {
                succeeded_count,
                failure_count,
            })?;

    if completed_count > task_count {
        return Err(BatchOutcomeBuildError::CompletedCountExceeded {
            task_count,
            completed_count,
        });
    }
    if terminal_count != completed_count {
        return Err(BatchOutcomeBuildError::TerminalCountMismatch {
            completed_count,
            terminal_count,
            succeeded_count,
            failed_count,
            panicked_count,
        });
    }
    if failures.len() != failure_count {
        return Err(BatchOutcomeBuildError::FailureDetailCountMismatch {
            expected: failure_count,
            actual: failures.len(),
        });
    }
    validate_failure_details(task_count, failed_count, panicked_count, failures)
}

/// Checks failure indices, uniqueness, ordering, and variant counts.
///
/// # Type Parameters
///
/// * `E` - Task-specific error type stored in failure records.
///
/// # Parameters
///
/// * `task_count` - Declared task count for the batch.
/// * `failed_count` - Expected number of business-error failure records.
/// * `panicked_count` - Expected number of panic failure records.
/// * `failures` - Failure details sorted by index when validation succeeds.
///
/// # Returns
///
/// `Ok(())` when every detail index is in range, unique, and matches the
/// expected failed and panicked counts.
///
/// # Errors
///
/// Returns [`BatchOutcomeBuildError`] when an index is out of range,
/// duplicated, or the detail variants do not match the aggregate counters.
fn validate_failure_details<E>(
    task_count: usize,
    failed_count: usize,
    panicked_count: usize,
    failures: &mut [BatchTaskFailure<E>],
) -> Result<(), BatchOutcomeBuildError> {
    let mut observed_failed_count = 0usize;
    let mut observed_panicked_count = 0usize;
    for failure in failures.iter() {
        if failure.index() >= task_count {
            return Err(BatchOutcomeBuildError::FailureIndexOutOfRange {
                index: failure.index(),
                task_count,
            });
        }
        match failure.error() {
            BatchTaskError::Failed(_) => observed_failed_count += 1,
            BatchTaskError::Panicked { .. } => observed_panicked_count += 1,
        }
    }
    failures.sort_unstable_by_key(BatchTaskFailure::index);
    for adjacent in failures.windows(2) {
        if adjacent[0].index() == adjacent[1].index() {
            return Err(BatchOutcomeBuildError::DuplicateFailureIndex {
                index: adjacent[0].index(),
            });
        }
    }
    if observed_failed_count != failed_count || observed_panicked_count != panicked_count {
        return Err(BatchOutcomeBuildError::FailureVariantCountMismatch {
            expected_failed: failed_count,
            actual_failed: observed_failed_count,
            expected_panicked: panicked_count,
            actual_panicked: observed_panicked_count,
        });
    }
    Ok(())
}
