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

use qubit_batch::BatchCallError;
use qubit_batch::BatchExecutionError;
use qubit_batch::SequentialBatchExecutor;
use qubit_function::Callable;

use crate::support::FailingReporter;

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

impl Callable<i32, CallableError> for ErrorCallable {
    fn call(&mut self) -> Result<i32, CallableError> {
        Err(CallableError)
    }
}

impl Callable<i32, CallableError> for MixedCallable {
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

#[test]
fn test_batch_call_error_source_exposes_execution_error() {
    let error = SequentialBatchExecutor::new()
        .call_with_count(vec![ErrorCallable], 2)
        .expect_err("short source should produce a batch-level error");
    let source = Error::source(&error).expect("execution error should be exposed");
    let execution = source
        .downcast_ref::<BatchExecutionError<CallableError>>()
        .expect("source should expose BatchExecutionError directly");
    assert!(execution.is_count_shortfall());
    assert_eq!(execution.outcome().completed_count(), 1);
    assert!(Error::source(execution).is_none());
}

struct ValueWithoutDebug;

fn assert_standard_error<T: Error>() {}

fn propagate_unit_call_error() -> Result<(), Box<dyn Error>> {
    let _result = SequentialBatchExecutor::new().call_with_count([|| Ok::<_, ()>(ValueWithoutDebug)], 2)?;
    Ok(())
}

#[test]
fn test_batch_call_error_accepts_debug_business_errors() {
    assert_standard_error::<BatchCallError<ValueWithoutDebug, ()>>();
    assert_standard_error::<BatchCallError<ValueWithoutDebug, &'static str>>();
    let error = propagate_unit_call_error().expect_err("shortfall should propagate");
    assert!(error.downcast_ref::<BatchCallError<ValueWithoutDebug, ()>>().is_some());
}

#[test]
fn test_batch_call_error_preserves_progress_source_chain() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(0))
        .build();
    let error = executor
        .call_with_count(vec![ErrorCallable], 1)
        .expect_err("start report should fail");
    let source = Error::source(&error).expect("execution source should exist");
    let execution = source
        .downcast_ref::<BatchExecutionError<CallableError>>()
        .expect("execution error should be directly accessible");
    let progress = Error::source(execution).expect("progress source should remain accessible");
    assert_eq!(
        progress.to_string(),
        execution
            .progress_report_error()
            .expect("progress accessor should retain the failure")
            .to_string(),
    );
    assert_eq!(error.outcome().completed_count(), 0);
}
