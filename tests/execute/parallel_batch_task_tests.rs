// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the typed task token used by parallel schedulers.

use std::sync::Arc;

use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_batch::execute::spi::ParallelBatchTask;
use qubit_progress::reporter::NoopReporter;

use crate::support::TestTask;

#[test]
fn test_parallel_batch_task_is_created_and_consumed_by_context() {
    let coordinator =
        ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), std::time::Duration::ZERO);
    let outcome = coordinator
        .execute(
            [TestTask::succeed()],
            1,
            TaskFailurePolicy::Continue,
            |tasks, context| {
                for task in tasks {
                    let token: Option<ParallelBatchTask<_>> = context.accept_task(task);
                    if let Some(token) = token {
                        context.execute_task(token);
                    }
                }
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect("accepted task token should execute successfully");

    assert_eq!(outcome.completed_count(), 1);
}
