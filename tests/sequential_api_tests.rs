// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![allow(clippy::result_large_err)]

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;

use qubit_batch::BatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_function::Callable;
use qubit_function::Runnable;

#[test]
fn test_sequential_for_each_borrows_mutable_local_state() {
    let mut values = Vec::new();
    let result = SequentialBatchExecutor::new()
        .for_each([1, 2, 3], |item| {
            values.push(item * 2);
            Ok::<(), ()>(())
        })
        .expect("exact-size input should execute successfully");
    assert!(result.is_success());
    assert_eq!(values, [2, 4, 6]);
}

#[test]
fn test_sequential_action_panic_is_a_task_failure() {
    let result = SequentialBatchExecutor::new()
        .for_each([0, 1, 2], |item| {
            if item == 1 {
                panic!("expected action panic");
            }
            Ok::<(), ()>(())
        })
        .expect("action panic should remain a task failure");
    assert_eq!(result.succeeded_count(), 2);
    assert_eq!(result.panicked_count(), 1);
    assert_eq!(result.failures()[0].index(), 1);
}

#[test]
fn test_generic_trait_call_preserves_sparse_outputs() {
    fn run<X: BatchExecutor>(executor: &X) {
        let result = executor
            .call((0..3).map(|index| move || if index == 1 { Err(()) } else { Ok(index) }))
            .expect("task failures should be represented in the call result");
        let indexes: Vec<_> = result
            .outputs()
            .iter()
            .map(|output| output.index())
            .collect();
        assert_eq!(indexes, [0, 2]);
    }

    run(&SequentialBatchExecutor::new());
}

#[test]
fn test_local_callable_result_and_shortfall_are_preserved() {
    let local = Rc::new(String::from("local"));
    let error = SequentialBatchExecutor::new()
        .call_with_count([|| Ok::<_, ()>(Rc::clone(&local))], 2)
        .expect_err("declared count should exceed the callable source");
    assert_eq!(error.outcome().succeeded_count(), 1);
    assert_eq!(error.outputs()[0].value().as_str(), "local");
}

#[test]
fn test_iterator_panic_propagates() {
    let items = std::iter::once(0).chain(std::iter::from_fn(|| -> Option<i32> {
        panic!("expected iterator panic");
    }));

    let result = catch_unwind(AssertUnwindSafe(|| {
        SequentialBatchExecutor::new().for_each_with_count(items, 2, |_| Ok::<(), ()>(()))
    }));

    assert!(result.is_err(), "iterator panic should propagate");
}

#[test]
fn test_runnable_drop_panic_propagates() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        SequentialBatchExecutor::new().execute_with_count([DropPanickingRunnable], 1)
    }));

    assert!(
        result.is_err(),
        "runnable destructor panic should propagate"
    );
}

#[test]
fn test_callable_drop_panic_is_captured() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        SequentialBatchExecutor::new().call_with_count([DropPanickingCallable], 1)
    }))
    .expect("callable destructor panic should be captured");
    let result = result.expect("task panic should remain in the callable result");

    assert_eq!(result.outcome().panicked_count(), 1);
    assert!(result.outputs().is_empty());
}

struct DropPanickingRunnable;

impl Runnable<()> for DropPanickingRunnable {
    fn run(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

impl Drop for DropPanickingRunnable {
    fn drop(&mut self) {
        panic!("expected runnable destructor panic");
    }
}

struct DropPanickingCallable;

impl Callable<(), ()> for DropPanickingCallable {
    fn call(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

impl Drop for DropPanickingCallable {
    fn drop(&mut self) {
        panic!("expected callable destructor panic");
    }
}
