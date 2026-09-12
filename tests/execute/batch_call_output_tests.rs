// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for sparse callable outputs preserved by batch-level errors.

use qubit_batch::BatchExecutionError;
use qubit_batch::SequentialBatchExecutor;

use crate::support::TestCallable;

#[test]
fn test_batch_call_output_exposes_index_and_value() {
    let error = SequentialBatchExecutor::new()
        .call_with_count([TestCallable::returning(42)], 2)
        .expect_err("declared shortfall should preserve one output");
    let output = error.into_outputs().pop().expect("one output should be present");

    assert_eq!(output.index(), 0);
    assert_eq!(output.value(), &42);
    assert_eq!(output.into_value(), 42);
}

#[test]
fn test_batch_call_error_returns_sparse_outputs() {
    let error = SequentialBatchExecutor::new()
        .call_with_count([TestCallable::returning(10), TestCallable::returning(20)], 3)
        .expect_err("declared shortfall should preserve successful outputs");

    assert!(matches!(error.source(), BatchExecutionError::CountShortfall { .. }));
    assert_eq!(error.outputs().len(), 2);
    assert_eq!(error.outputs()[0].index(), 0);
    assert_eq!(error.outputs()[0].value(), &10);
    let outputs = error.into_outputs();
    assert_eq!(outputs[1].index(), 1);
    assert_eq!(outputs[1].value(), &20);
}
