// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::Cell;
use std::convert::Infallible;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchOutcome;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_function::Runnable;

use crate::support::FailingReporter;
use crate::support::TestTask;

/// Counts user source conversions without making the source `Send`.
struct CountedSource<'a> {
    conversions: &'a Cell<usize>,
    values: Vec<usize>,
}

impl IntoIterator for CountedSource<'_> {
    type Item = usize;
    type IntoIter = std::vec::IntoIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        self.conversions.set(self.conversions.get() + 1);
        self.values.into_iter()
    }
}

/// Rejects a source before pulling any item from it.
struct RejectForEachSource;

impl BatchExecutor for RejectForEachSource {
    type SchedulerError = io::Error;

    fn execute_with_count<T, E, I>(
        &self,
        _tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        Err(BatchExecutionError::ScheduleFailed {
            source: io::Error::other("rejected before source consumption"),
            outcome: BatchOutcomeBuilder::builder(count)
                .build()
                .expect("zero completions form a valid partial outcome"),
            report_error: None,
        })
    }
}

#[test]
fn test_batch_executor_trait_execute_dispatches_to_sequential_executor() {
    fn execute_one<E: BatchExecutor>(executor: &E) -> usize {
        executor
            .execute(vec![TestTask::succeed()])
            .expect("task should succeed")
            .completed_count()
    }

    let executor = SequentialBatchExecutor::new();
    assert_eq!(execute_one(&executor), 1);
}

#[test]
fn test_for_each_rejection_does_not_convert_source() {
    let conversions = Cell::new(0);
    let error = BatchExecutor::for_each_with_count(
        &RejectForEachSource,
        CountedSource {
            conversions: &conversions,
            values: vec![2, 3, 4],
        },
        3,
        |_| Ok::<(), ()>(()),
    )
    .expect_err("the executor rejects before consuming the source");
    assert!(error.is_schedule_failed());
    assert_eq!(error.outcome().completed_count(), 0);
    assert_eq!(conversions.get(), 0, "rejection must skip user conversion");
}

#[test]
fn test_for_each_start_failure_matches_concrete_sequential_behavior() {
    let executor = SequentialBatchExecutor::builder()
        .reporter(FailingReporter::after_successes(0))
        .build();
    let concrete_conversions = Cell::new(0);
    let concrete_error = executor
        .for_each_with_count(
            CountedSource {
                conversions: &concrete_conversions,
                values: vec![2],
            },
            1,
            |_| Ok::<(), ()>(()),
        )
        .expect_err("progress start must fail");
    assert!(matches!(concrete_error, BatchExecutionError::ProgressReport { .. }));
    assert_eq!(concrete_conversions.get(), 0);

    let trait_conversions = Cell::new(0);
    let trait_error = BatchExecutor::for_each_with_count(
        &executor,
        CountedSource {
            conversions: &trait_conversions,
            values: vec![2],
        },
        1,
        |_| Ok::<(), ()>(()),
    )
    .expect_err("progress start must fail through the trait too");
    assert!(matches!(trait_error, BatchExecutionError::ProgressReport { .. }));
    assert_eq!(trait_conversions.get(), 0, "trait dispatch must skip conversion");
}

/// Checks successful consumption through the trait default adapter.
fn assert_for_each_converts_once<X: BatchExecutor<SchedulerError = Infallible>>(executor: &X) {
    let conversions = Cell::new(0);
    let sum = AtomicUsize::new(0);
    let outcome = BatchExecutor::for_each_with_count(
        executor,
        CountedSource {
            conversions: &conversions,
            values: vec![2, 3, 4],
        },
        3,
        |value| {
            sum.fetch_add(value, Ordering::Relaxed);
            Ok::<(), ()>(())
        },
    )
    .expect("the declared count matches the source");
    assert!(outcome.is_success());
    assert_eq!(outcome.completed_count(), 3);
    assert_eq!(outcome.succeeded_count(), 3);
    assert_eq!(conversions.get(), 1, "consumption converts the source once");
    assert_eq!(sum.load(Ordering::Relaxed), 9);
}

#[test]
fn test_for_each_converts_once_on_sequential_and_parallel_paths() {
    assert_for_each_converts_once(&SequentialBatchExecutor::new());
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("two workers are valid");
    assert_for_each_converts_once(&parallel);
}
