// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Baseline benchmarks for sequential and scoped-thread batch execution.

use std::hint::black_box;
use std::rc::Rc;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchCallOutput;
use qubit_batch::BatchCallResult;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;
use qubit_batch::BatchProcessor;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::ParallelBatchProcessor;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::SequentialBatchProcessor;
use qubit_function::Callable;
use qubit_function::Runnable;

/// Batch sizes around the default sequential execution threshold.
const BATCH_SIZES: [usize; 7] = [32, 64, 99, 100, 101, 128, 256];

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
            value = value.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        }
        black_box(value);
        Ok(())
    }
}

/// Callable that measures indexed output collection without application work.
fn constant_callable() -> Result<u64, ()> {
    Ok(1)
}

/// Callable that proves the sequential inherent API accepts non-`Send` values.
struct NonSendCallable {
    /// Marker that deliberately makes this callable non-`Send`.
    marker: Rc<()>,
}

impl Callable<(), ()> for NonSendCallable {
    /// Completes without moving the non-`Send` marker across a thread.
    fn call(&mut self) -> Result<(), ()> {
        let _ = Rc::strong_count(&self.marker);
        Ok(())
    }
}

/// Callable that returns a preconstructed fixed-size value.
struct LargeValueCallable {
    /// Value prepared outside the measured benchmark iteration.
    value: Vec<u8>,
}

impl Callable<Vec<u8>, ()> for LargeValueCallable {
    /// Returns the prepared value and leaves an empty vector in the callable.
    fn call(&mut self) -> Result<Vec<u8>, ()> {
        Ok(std::mem::take(&mut self.value))
    }
}

/// Size of each preconstructed return value in the large-value benchmark.
const LARGE_VALUE_SIZE: usize = 4 * 1024;

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
    let default_parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
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
        group.bench_with_input(
            BenchmarkId::new("default_threshold", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        default_parallel
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
    let default_parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
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
                            .execute_with_count((0..task_count).map(|seed| CpuTask { seed: seed as u64 }), task_count)
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
                            .execute_with_count((0..task_count).map(|seed| CpuTask { seed: seed as u64 }), task_count)
                            .expect("CPU batch should succeed"),
                    );
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("default_threshold", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let _ = black_box(
                        default_parallel
                            .execute_with_count((0..task_count).map(|seed| CpuTask { seed: seed as u64 }), task_count)
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
    let default_parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
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
        group.bench_with_input(
            BenchmarkId::new("default_threshold", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = default_parallel
                        .call((0..task_count).map(|_| constant_callable))
                        .expect("callable batch should succeed");
                    let _ = black_box(result);
                });
            },
        );
    }
    group.finish();
}

/// Benchmarks validation of sparse callable outputs and ordered failures.
///
/// Input construction is performed by `iter_batched` setup and is excluded
/// from the measured validation path.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_call_validation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("batch_call_validation");

    for task_count in [1_000usize, 10_000, 100_000] {
        group.bench_function(BenchmarkId::new("mixed", task_count), |bencher| {
            bencher.iter_batched(
                || {
                    let failures = (1..task_count)
                        .step_by(2)
                        .map(|index| BatchTaskFailure::new(index, BatchTaskError::Failed(())))
                        .collect();
                    let outcome = BatchOutcomeBuilder::builder(task_count)
                        .completed_count(task_count)
                        .succeeded_count(task_count / 2)
                        .failed_count(task_count / 2)
                        .failures(failures)
                        .build()
                        .expect("mixed outcome should be valid");
                    let outputs = (0..task_count)
                        .step_by(2)
                        .map(|index| BatchCallOutput::new(index, index))
                        .collect();
                    (outcome, outputs)
                },
                |(outcome, outputs)| {
                    let _ = black_box(
                        BatchCallResult::try_new(outcome, outputs)
                            .expect("mixed result should be valid"),
                    );
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function(BenchmarkId::new("all_success", task_count), |bencher| {
            bencher.iter_batched(
                || {
                    let outcome = BatchOutcomeBuilder::<()>::builder(task_count)
                        .completed_count(task_count)
                        .succeeded_count(task_count)
                        .build()
                        .expect("all-success outcome should be valid");
                    let outputs = (0..task_count)
                        .map(|index| BatchCallOutput::new(index, index))
                        .collect();
                    (outcome, outputs)
                },
                |(outcome, outputs)| {
                    let _ = black_box(
                        BatchCallResult::try_new(outcome, outputs)
                            .expect("all-success result should be valid"),
                    );
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Benchmarks callable collection for non-`Send` and large return values.
///
/// Non-`Send` callables use the concrete sequential API. Large return values
/// are prepared outside the measured iteration; their drop is included when
/// the result leaves the measured closure.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_callable_value_shapes(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let default_parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .build()
        .expect("benchmark executor configuration should be valid");
    let marker = Rc::new(());
    let mut group = criterion.benchmark_group("batch_executor_callable_values");

    for task_count in BATCH_SIZES {
        group.bench_with_input(
            BenchmarkId::new("sequential_non_send", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter_batched(
                    || {
                        (0..task_count)
                            .map(|_| NonSendCallable {
                                marker: Rc::clone(&marker),
                            })
                            .collect::<Vec<_>>()
                    },
                    |tasks| {
                        let result = sequential
                            .call(tasks)
                            .expect("non-Send callable batch should succeed");
                        let _ = black_box(result);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("sequential_large_value", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter_batched(
                    || {
                        (0..task_count)
                            .map(|_| LargeValueCallable {
                                value: vec![0xA5; LARGE_VALUE_SIZE],
                            })
                            .collect::<Vec<_>>()
                    },
                    |tasks| {
                        let result = sequential
                            .call(tasks)
                            .expect("large-value callable batch should succeed");
                        let _ = black_box(result);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("scoped_parallel_large_value", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter_batched(
                    || {
                        (0..task_count)
                            .map(|_| LargeValueCallable {
                                value: vec![0xA5; LARGE_VALUE_SIZE],
                            })
                            .collect::<Vec<_>>()
                    },
                    |tasks| {
                        let result = parallel
                            .call(tasks)
                            .expect("parallel large-value callable batch should succeed");
                        let _ = black_box(result);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("default_threshold_large_value", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter_batched(
                    || {
                        (0..task_count)
                            .map(|_| LargeValueCallable {
                                value: vec![0xA5; LARGE_VALUE_SIZE],
                            })
                            .collect::<Vec<_>>()
                    },
                    |tasks| {
                        let result = default_parallel
                            .call(tasks)
                            .expect("default large-value callable batch should succeed");
                        let _ = black_box(result);
                    },
                    BatchSize::LargeInput,
                );
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
    let mut default_parallel = ParallelBatchProcessor::builder(|_: &u64| {})
        .thread_count(4)
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
        group.bench_with_input(
            BenchmarkId::new("default_threshold", task_count),
            &task_count,
            |bencher, &task_count| {
                bencher.iter(|| {
                    let result = default_parallel
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
    benchmark_call_validation,
    benchmark_callable_value_shapes,
    benchmark_item_processing,
);
criterion_main!(benches);
