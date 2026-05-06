/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Integration tests for [`BatchExecutor::for_each`](qubit_batch::BatchExecutor::for_each)
//! and the internal per-item runnable wrapper.

use qubit_batch::{BatchExecutor, SequentialBatchExecutor};

#[test]
fn test_sequential_batch_executor_for_each_maps_items() {
    let executor = SequentialBatchExecutor::new();

    let result = executor
        .for_each(0..4, 4, |value| {
            if value == 2 {
                Err("bad item")
            } else {
                Ok::<(), &'static str>(())
            }
        })
        .expect("for_each should keep item failures in the result");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.succeeded_count(), 3);
    assert_eq!(result.failed_count(), 1);
    assert_eq!(result.failures()[0].index(), 2);
}
