/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Behavioral coverage for the scoped parallel runner.

use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    Mutex,
};

use qubit_batch::{
    BatchProcessor,
    ParallelBatchProcessor,
};

#[test]
fn test_scoped_parallel_runner_preserves_item_processing() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = ParallelBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    })
    .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));

    let result = processor
        .process(vec![1, 2, 3, 4], 4)
        .expect("parallel processing should succeed");
    let mut accepted = accepted
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    accepted.sort_unstable();

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.processed_count(), 4);
    assert_eq!(accepted, vec![1, 2, 3, 4]);
}
