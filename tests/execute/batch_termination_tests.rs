// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`BatchTermination`](qubit_batch::BatchTermination).

use qubit_batch::BatchTermination;

#[test]
fn test_batch_termination_defaults_to_finished() {
    assert_eq!(BatchTermination::default(), BatchTermination::Finished);
}
