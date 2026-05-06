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
    BatchTaskError,
    BatchTaskFailure,
};

#[test]
fn test_batch_task_failure_accessors_and_into_error() {
    let failure = BatchTaskFailure::new(7, BatchTaskError::Failed("failed"));

    assert_eq!(failure.index(), 7);
    assert_eq!(failure.error(), &BatchTaskError::Failed("failed"));
    assert_eq!(failure.into_error(), BatchTaskError::Failed("failed"));
}
