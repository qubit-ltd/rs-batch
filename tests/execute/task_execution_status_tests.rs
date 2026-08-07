// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0.
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression coverage for task terminal status accounting.

use qubit_batch::SequentialBatchExecutor;

use crate::support::TestTask;

#[test]
fn test_task_execution_status_accounts_failed_tasks_once() {
    let outcome = SequentialBatchExecutor::new()
        .execute_with_count([TestTask::succeed(), TestTask::fail("failed")], 2)
        .expect("task statuses should produce a valid outcome");

    assert_eq!(outcome.completed_count(), 2);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
}
