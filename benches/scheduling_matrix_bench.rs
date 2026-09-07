// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures executor cost across task sizes, work shapes, and callers.

use std::hint::black_box;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchExecutor;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;

/// Executes deterministic CPU work, optionally skewed toward every 16th item.
fn work(seed: usize, rounds: usize, skewed: bool) {
    let rounds = if skewed && seed.is_multiple_of(16) {
        rounds * 16
    } else {
        rounds
    };
    let mut value = black_box(seed as u64);
    for step in 0..rounds {
        value = black_box(value.rotate_left(7).wrapping_mul(6_364_136_223_846_793_005) ^ step as u64);
    }
    black_box(value);
}

/// Measures one call across source sizes and deterministic work shapes.
fn register_executor<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(label);
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    for count in [32usize, 100, 101, 1024] {
        for rounds in [0usize, 64, 4096] {
            for skewed in [false, true] {
                let id = BenchmarkId::new(format!("rounds-{rounds}-skew-{skewed}"), count);
                group.bench_function(id, |bencher| {
                    let check = executor
                        .for_each_with_count(0..count, count, |index| {
                            work(index, rounds, skewed);
                            Ok::<(), ()>(())
                        })
                        .expect("setup batch should succeed");
                    assert_eq!(check.succeeded_count(), count);
                    bencher.iter(|| {
                        let outcome = executor
                            .for_each_with_count(0..count, count, |index| {
                                work(index, rounds, skewed);
                                Ok::<(), ()>(())
                            })
                            .expect("benchmark batch should succeed");
                        let _ = black_box(outcome);
                    });
                });
            }
        }
    }
    group.finish();
}

/// Measures end-to-end concurrent calls, including caller thread creation.
fn register_concurrent<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(format!("end-to-end-concurrent-{label}"));
    group.sample_size(30);
    for callers in [2usize, 4] {
        group.bench_function(BenchmarkId::from_parameter(callers), |bencher| {
            thread::scope(|scope| {
                let handles: Vec<_> = (0..callers)
                    .map(|_| {
                        scope.spawn(|| {
                            executor
                                .for_each_with_count(0..1024, 1024, |index| {
                                    work(index, 64, true);
                                    Ok::<(), ()>(())
                                })
                                .expect("setup batch should succeed")
                        })
                    })
                    .collect();
                for handle in handles {
                    assert_eq!(handle.join().expect("setup caller should join").succeeded_count(), 1024);
                }
            });
            bencher.iter(|| {
                std::thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(callers);
                    for _ in 0..callers {
                        handles.push(scope.spawn(|| {
                            executor
                                .for_each_with_count(0..1024, 1024, |index| {
                                    work(index, 64, true);
                                    Ok::<(), ()>(())
                                })
                                .expect("concurrent benchmark batch should succeed")
                                .completed_count()
                        }));
                    }
                    for handle in handles {
                        black_box(handle.join().expect("benchmark caller should join"));
                    }
                });
            });
        });
    }
    group.finish();
}

/// Measures batches dispatched by caller threads created before timing starts.
/// Queue handoff is part of this workload; caller thread creation and joins are
/// excluded. One untimed round validates every caller before measurement.
fn register_steady_callers<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(format!("steady-callers-{label}"));
    group.sample_size(30);
    for callers in [2usize, 4] {
        group.bench_function(BenchmarkId::from_parameter(callers), |bencher| {
            thread::scope(|scope| {
                let mut connections = Vec::with_capacity(callers);
                let mut handles = Vec::with_capacity(callers);
                for _ in 0..callers {
                    let (request_sender, request_receiver) = mpsc::channel::<()>();
                    let (result_sender, result_receiver) = mpsc::channel();
                    handles.push(scope.spawn(move || {
                        while request_receiver.recv().is_ok() {
                            let outcome = executor
                                .for_each_with_count(0..1024, 1024, |index| {
                                    work(index, 64, true);
                                    Ok::<(), ()>(())
                                })
                                .expect("steady caller batch should succeed");
                            if result_sender.send(outcome.completed_count()).is_err() {
                                break;
                            }
                        }
                    }));
                    connections.push((request_sender, result_receiver));
                }
                for (sender, _) in &connections {
                    sender.send(()).expect("caller must accept setup work");
                }
                for (_, receiver) in &connections {
                    assert_eq!(receiver.recv().expect("setup work must finish"), 1024);
                }
                bencher.iter(|| {
                    for (sender, _) in &connections {
                        sender.send(()).expect("caller must accept measured work");
                    }
                    for (_, receiver) in &connections {
                        black_box(receiver.recv().expect("measured work must finish"));
                    }
                });
                drop(connections);
                for handle in handles {
                    handle.join().expect("steady caller must join");
                }
            });
        });
    }
    group.finish();
}

/// Registers dispatch, end-to-end caller, and steady caller workloads.
fn benchmarks(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    register_executor(criterion, "sequential", &sequential);
    register_concurrent(criterion, "sequential", &sequential);
    register_steady_callers(criterion, "sequential", &sequential);
    for threads in [2usize, 4] {
        let executor = ParallelBatchExecutor::builder()
            .thread_count(threads)
            .sequential_threshold(0)
            .build()
            .expect("scoped executor should build");
        register_executor(criterion, &format!("scoped-{threads}"), &executor);
        register_concurrent(criterion, &format!("scoped-{threads}"), &executor);
        register_steady_callers(criterion, &format!("scoped-{threads}"), &executor);
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
