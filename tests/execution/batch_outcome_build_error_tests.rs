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
    BatchOutcome,
    BatchOutcomeBuildError,
};
use std::time::Duration;

#[test]
fn test_batch_outcome_build_error_terminal_count_mismatch() {
    let error = BatchOutcome::<&'static str>::try_new(2, 2, 1, 0, 0, Duration::ZERO, Vec::new())
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
