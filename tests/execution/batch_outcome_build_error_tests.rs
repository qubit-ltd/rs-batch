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
    BatchOutcomeBuildError,
    BatchOutcomeBuilder,
};

#[test]
fn test_batch_outcome_build_error_terminal_count_mismatch() {
    let error = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .build()
        .expect_err("terminal count mismatch should be rejected");

    assert!(matches!(
        error,
        BatchOutcomeBuildError::TerminalCountMismatch { .. }
    ));
    assert!(
        error
            .to_string()
            .contains("completed task count must equal")
    );
}
