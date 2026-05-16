/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Behavioral coverage for execution and processing state counters.

use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    BatchProcessor,
    ParallelBatchExecutor,
    ParallelBatchProcessor,
};

use crate::support::TestTask;

#[test]
fn test_batch_counter_supports_execution_and_processing_counts() {
    let executor = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("parallel executor should build");
    let outcome = executor
        .execute_with_count(
            [
                TestTask::sleep_success(Duration::from_millis(1)),
                TestTask::succeed(),
            ],
            2,
        )
        .expect("parallel execution should succeed");

    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(NonZeroUsizeExt::two())
        .build();
    let process_result = processor
        .process_with_count(vec![1, 2], 2)
        .expect("parallel processing should succeed");

    assert_eq!(outcome.completed_count(), 2);
    assert_eq!(outcome.succeeded_count(), 2);
    assert_eq!(process_result.completed_count(), 2);
    assert_eq!(process_result.processed_count(), 2);
}

/// Helpers for concise non-zero constants in tests.
struct NonZeroUsizeExt;

impl NonZeroUsizeExt {
    /// Returns a non-zero value of two.
    ///
    /// # Returns
    ///
    /// A non-zero worker count.
    fn two() -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(2).expect("two is non-zero")
    }
}
