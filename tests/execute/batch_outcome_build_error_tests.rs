// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_batch::BatchOutcomeBuildError;
use qubit_batch::BatchOutcomeBuilder;

#[test]
fn test_batch_outcome_build_error_terminal_count_mismatch() {
    let error = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .build()
        .expect_err("terminal count mismatch should be rejected");

    assert_eq!(
        error,
        BatchOutcomeBuildError::TerminalCountMismatch {
            completed_count: 2,
            terminal_count: 1,
            succeeded_count: 1,
            failed_count: 0,
            panicked_count: 0,
        }
    );
    assert_eq!(
        error.to_string(),
        "completed task count must equal succeeded + failed + panicked counts: completed_count 2, terminal_count 1"
    );
}
