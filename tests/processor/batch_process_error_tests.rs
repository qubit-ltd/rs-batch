/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`BatchProcessError`](qubit_batch::BatchProcessError).

use std::time::Duration;

use qubit_batch::{BatchProcessError, BatchProcessResult};

#[test]
fn test_batch_process_error_helpers_and_display() {
    let result = BatchProcessResult::new(3, 1, 1, 1, Duration::from_millis(5));
    let shortfall = BatchProcessError::CountShortfall {
        expected: 3,
        actual: 1,
        result: result.clone(),
    };
    let exceeded = BatchProcessError::CountExceeded {
        expected: 3,
        observed_at_least: 4,
        result: result.clone(),
    };

    assert_eq!(shortfall.result(), &result);
    assert_eq!(shortfall.clone().into_result(), result);
    assert_eq!(
        shortfall.to_string(),
        "batch item count shortfall: expected 3, actual 1"
    );
    assert_eq!(exceeded.result(), &result);
    assert_eq!(exceeded.clone().into_result(), result);
    assert_eq!(
        exceeded.to_string(),
        "batch item count exceeded: expected 3, observed at least 4"
    );
}
