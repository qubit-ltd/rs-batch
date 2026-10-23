// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::NoopReporter;

#[test]
fn test_execute_target_runs_a_batch() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let outcome = coordinator
        .execute_with_source(
            [|| Ok::<(), Infallible>(())],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    context.execute_task(task);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect("the task should execute successfully");

    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(outcome.succeeded_count(), 1);
}
