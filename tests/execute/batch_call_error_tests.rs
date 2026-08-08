// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for callable errors and their sparse preserved outputs.

use std::error::Error;
use std::fmt;

use qubit_batch::SequentialBatchExecutor;

#[derive(Debug, PartialEq, Eq)]
struct CallableError;

impl fmt::Display for CallableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("callable error")
    }
}

impl Error for CallableError {}

struct ErrorCallable;

enum MixedCallable {
    Success(i32),
    Error,
}

impl qubit_function::Callable<i32, CallableError> for ErrorCallable {
    fn call(&mut self) -> Result<i32, CallableError> {
        Err(CallableError)
    }
}

impl qubit_function::Callable<i32, CallableError> for MixedCallable {
    fn call(&mut self) -> Result<i32, CallableError> {
        match self {
            Self::Success(value) => Ok(*value),
            Self::Error => Err(CallableError),
        }
    }
}

#[test]
fn test_batch_call_error_accessors_and_error_traits() {
    let executor = SequentialBatchExecutor::new();
    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");

    assert!(error.outputs().is_empty());
    assert_eq!(error.outcome().completed_count(), 1);
    assert!(!error.to_string().is_empty());
    assert!(format!("{error:?}").contains("output_count"));
    assert!(Error::source(&error).is_some());
    assert!(!error.source().to_string().is_empty());

    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");
    assert!(error.into_outputs().is_empty());

    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");
    assert!(error.into_source().is_count_shortfall());
}

#[test]
fn test_batch_call_error_preserves_sparse_success_outputs() {
    let executor = SequentialBatchExecutor::new();
    let error = executor
        .call_with_count(
            vec![
                MixedCallable::Success(10),
                MixedCallable::Error,
                MixedCallable::Success(30),
            ],
            4,
        )
        .expect_err("call shortfall should preserve successful outputs");

    assert_eq!(error.outputs().len(), 2);
    assert_eq!(error.outputs()[0].index(), 0);
    assert_eq!(error.outputs()[0].value(), &10);
    assert_eq!(error.outputs()[1].index(), 2);
    assert_eq!(error.outputs()[1].value(), &30);

    let (source, outputs) = error.into_parts();
    assert!(source.is_count_shortfall());
    let mut outputs = outputs.into_iter();
    assert_eq!(outputs.next().expect("first output").into_parts(), (0, 10));
    assert_eq!(outputs.next().expect("second output").into_parts(), (2, 30));
}
