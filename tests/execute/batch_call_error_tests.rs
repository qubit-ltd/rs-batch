// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral coverage for callable errors and their preserved values.

use std::{
    error::Error,
    fmt,
};

use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};

#[derive(Debug, PartialEq, Eq)]
struct CallableError;

impl fmt::Display for CallableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("callable error")
    }
}

impl Error for CallableError {}

struct ErrorCallable;

impl qubit_function::Callable<i32, CallableError> for ErrorCallable {
    fn call(&mut self) -> Result<i32, CallableError> {
        Err(CallableError)
    }
}

#[test]
fn test_batch_call_error_accessors_and_error_traits() {
    let executor = SequentialBatchExecutor::new();
    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");

    assert_eq!(error.values(), &[None, None]);
    assert_eq!(error.outcome().completed_count(), 1);
    assert!(!error.to_string().is_empty());
    assert!(format!("{error:?}").contains("value_count"));
    assert!(Error::source(&error).is_some());
    assert!(!error.source().to_string().is_empty());

    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");
    assert_eq!(error.into_values(), vec![None, None]);

    let error = executor
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("call shortfall should return a callable error");
    assert!(error.into_source().is_count_shortfall());
}
