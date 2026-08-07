// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Baseline benchmarks for sequential and scoped-thread batch execution.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use qubit_batch::{
    BatchExecutor, BatchProcessor, ParallelBatchExecutor, ParallelBatchProcessor,
    SequentialBatchExecutor, SequentialBatchProcessor,
};
use qubit_function::Runnable;

/// Batch sizes around the default sequential execution threshold.
const BATCH_SIZES: [usize; 5] = [32, 64, 100, 128, 256];

/// Task that measures executor dispatch overhead without application work.
#[derive(Clone, Copy)]
struct NoOpTask;

impl Runnable<()> for NoOpTask {
    /// Completes without performing application work.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    #[inline]
    fn run(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

/// Task that performs deterministic CPU work before completing.
#[derive(Clone, Copy)]
struct CpuTask {
    /// Per-task seed that prevents identical loop inputs.
    seed: u64,
}

impl Runnable<()> for CpuTask {
    /// Performs a bounded CPU workload.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` after black-boxing the calculated value.
    #[inline]
    fn run(&mut self) -> Result<(), ()> {
        let mut value = self.seed;
        for _ in 0..256 {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
        }
        black_box(value);
        Ok(())
    }
}

/// Callable that measures indexed output collection without application work.
fn constant_callable() -> Result<u64, ()> {
    Ok(1)
}

/// Benchmarks executor overhead for no-op tasks near the dispatch threshold.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_no_op_execution(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_executor_no_op");

    for task_count in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("sequential", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        sequential
                            .execute_with_count((0..task_count).map(|_| NoOpTask), task_count)
                            .expect("no-op batch should succeed"),
                    );
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scoped_parallel", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        parallel
                            .execute_with_count((0..task_count).map(|_| NoOpTask), task_count)
                            .expect("no-op batch should succeed"),
                    );
                });
            },
        );
    }
    group.finish();
}

/// Benchmarks sequential and scoped-thread execution for CPU-bound tasks.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_cpu_execution(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_executor_cpu");

    for task_count in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("sequential", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        sequential
                            .execute_with_count(
                                (0..task_count).map(|seed| CpuTask { seed: seed as u64 }),
                                task_count,
                            )
                            .expect("CPU batch should succeed"),
                    );
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scoped_parallel", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        parallel
                            .execute_with_count(
                                (0..task_count).map(|seed| CpuTask { seed: seed as u64 }),
                                task_count,
                            )
                            .expect("CPU batch should succeed"),
                    );
                });
            },
        );
    }
    group.finish();
}

/// Benchmarks callable output collection for sequential and parallel executors.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_callable_execution(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_executor_callable");

    for task_count in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("sequential", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = sequential
                        .call((0..task_count).map(|_| constant_callable))
                        .expect("callable batch should succeed");
                    let _ = black_box(result);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scoped_parallel", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = parallel
                        .call((0..task_count).map(|_| constant_callable))
                        .expect("callable batch should succeed");
                    let _ = black_box(result);
                });
            },
        );
    }
    group.finish();
}

/// Benchmarks direct item processing for sequential and parallel processors.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_item_processing(criterion: &mut Criterion) {
    let mut sequential = SequentialBatchProcessor::new(|_: &u64| {});
    let mut parallel = ParallelBatchProcessor::builder(|_: &u64| {})
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark processor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_processor");

    for task_count in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("sequential", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = sequential
                        .process_with_count((0..task_count).map(|value| value as u64), task_count)
                        .expect("sequential batch should succeed");
                    let _ = black_box(result);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scoped_parallel", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = parallel
                        .process_with_count((0..task_count).map(|value| value as u64), task_count)
                        .expect("parallel batch should succeed");
                    let _ = black_box(result);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_no_op_execution,
    benchmark_cpu_execution,
    benchmark_callable_execution,
    benchmark_item_processing,
);
criterion_main!(benches);
