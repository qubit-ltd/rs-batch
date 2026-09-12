// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Callable outputs and thread placement across fallback and parallel paths.
#![allow(clippy::result_large_err)]
use std::io;
use std::thread;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::ParallelBatchExecutor as Executor;
use qubit_function::Callable;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

#[derive(Debug)]
struct NonClone {
    value: usize,
}

#[test]
fn test_callable_matrix_preserves_values_and_thread_placement() {
    for (workers, threshold) in [(2, 100), (2, 0), (1, 0)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("valid executor");
        for count in [0usize, 1, 32, 100, 101] {
            let caller = thread::current().id();
            let sequential = workers == 1 || count <= threshold;
            let result = executor
                .call((0..count).map(|index| {
                    move || {
                        assert_eq!(thread::current().id() == caller, sequential);
                        Ok::<_, ()>(NonClone { value: index })
                    }
                }))
                .expect("exact batch");
            assert!(result.outcome().is_success());
            assert_eq!(result.outputs().len(), count);
            for (index, output) in result.outputs().iter().enumerate() {
                assert_eq!(output.index(), index);
                assert_eq!(output.value().value, index);
            }
        }
    }
}

struct DropCallable {
    index: usize,
}
impl Callable<NonClone, &'static str> for DropCallable {
    fn call(&mut self) -> Result<NonClone, &'static str> {
        if self.index == 1 {
            panic!("body panic");
        }
        if self.index == 3 {
            return Err("task error");
        }
        Ok(NonClone { value: self.index })
    }
}
impl Drop for DropCallable {
    fn drop(&mut self) {
        if self.index == 2 {
            panic!("drop panic");
        }
    }
}
#[test]
fn test_callable_panics_and_errors_preserve_sparse_successes() {
    for threshold in [0, 100] {
        let executor = Executor::builder()
            .thread_count(2)
            .sequential_threshold(threshold)
            .build()
            .expect("valid executor");
        let result = executor
            .call((0..5).map(|index| DropCallable { index }))
            .expect("task outcomes");
        assert_eq!(result.outcome().panicked_count(), 2);
        assert_eq!(result.outcome().failed_count(), 1);
        assert_eq!(result.outcome().completed_count(), 5);
        assert_eq!(
            result.outputs().iter().map(|o| o.index()).collect::<Vec<_>>(),
            vec![0, 4]
        );
        assert_eq!(
            result
                .outcome()
                .failures()
                .iter()
                .map(|f| f.index())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
}

/// Reports a terminal failure only, making error precedence deterministic.
struct TerminalFailure;
impl Reporter for TerminalFailure {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if matches!(event.phase(), Phase::Succeeded | Phase::Failed) {
            Err(ReporterError::new(io::Error::other("terminal rejected")))
        } else {
            Ok(())
        }
    }
}
#[test]
fn test_count_and_report_errors_preserve_outputs() {
    for threshold in [0, 100] {
        for expected in [1usize, 2, 3] {
            let executor = Executor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .reporter(TerminalFailure)
                .build()
                .expect("valid executor");
            let error = executor
                .call_with_count((0..2).map(|value| move || Ok::<_, ()>(NonClone { value })), expected)
                .expect_err("count or terminal error");
            let completed = expected.min(2);
            assert_eq!(error.outcome().completed_count(), completed);
            assert_eq!(error.outputs().len(), completed);
            assert_eq!(error.outputs()[0].value().value, 0);
            match (expected, error.source()) {
                (1, BatchExecutionError::CountExceeded { report_error, .. })
                | (3, BatchExecutionError::CountShortfall { report_error, .. }) => {
                    assert!(report_error.is_some())
                }
                (2, BatchExecutionError::ProgressReport { .. }) => {}
                (_, error) => panic!("unexpected error: {error:?}"),
            }
        }
    }
}
