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
    BatchExecutionState,
    BatchTaskError,
};
use std::time::Duration;

#[test]
fn test_batch_execution_state_counts_progress_and_outcome() {
    let mut state = BatchExecutionState::new(3);
    assert_eq!(state.task_count(), 3);
    assert_eq!(state.completed_count(), 0);

    state.record_task_started();
    state.record_task_succeeded();
    state.record_task_started();
    state.record_task_failed(1, "failed");
    state.record_task_started();
    state.record_task_panicked(2, BatchTaskError::panicked("boom"));

    let counters = state.progress_counters();
    assert_eq!(counters.total_count(), Some(3));
    assert_eq!(counters.completed_count(), 3);
    assert_eq!(counters.succeeded_count(), 1);
    assert_eq!(counters.failed_count(), 2);

    let outcome = state.into_outcome(Duration::from_millis(5));
    assert_eq!(outcome.failure_count(), 2);
    assert_eq!(outcome.failures()[0].index(), 1);
    assert_eq!(outcome.failures()[1].index(), 2);
}
