/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::{BatchExecutor, SequentialBatchExecutor};

use crate::support::TestTask;

#[test]
fn test_batch_executor_trait_execute_dispatches_to_sequential_executor() {
    fn execute_one<E: BatchExecutor>(executor: &E) -> usize {
        executor
            .execute(vec![TestTask::succeed()], 1)
            .expect("task should succeed")
            .completed_count()
    }

    let executor = SequentialBatchExecutor::new();
    assert_eq!(execute_one(&executor), 1);
}
