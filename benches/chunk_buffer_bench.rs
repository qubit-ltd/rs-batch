// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures reusable chunk storage without input collection in the timed
//! region.
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessor;

struct Consume;
impl BatchProcessor<u64> for Consume {
    type Error = std::convert::Infallible;
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = u64>,
    {
        for item in items {
            black_box(item);
        }
        Ok(BatchProcessResult::builder(count)
            .completed_count(count)
            .processed_count(count)
            .chunk_count(usize::from(count > 0))
            .build()
            .expect("complete chunk"))
    }
}
/// Registers total-size and chunk-size combinations with untimed validation.
fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk-buffer");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for count in [1024usize, 65536] {
        for size in [1usize, 16, 256, 4096] {
            let mut processor =
                ChunkedBatchProcessor::new(Consume, NonZeroUsize::new(size).expect("nonzero"));
            let check = processor
                .process_with_count(0..count as u64, count)
                .expect("setup");
            assert_eq!(check.completed_count(), count);
            assert_eq!(check.chunk_count(), count.div_ceil(size));
            group.bench_function(BenchmarkId::new(format!("size-{size}"), count), |b| {
                b.iter(|| {
                    black_box(
                        processor
                            .process_with_count(0..count as u64, count)
                            .expect("measured batch"),
                    )
                });
            });
        }
    }
    group.finish();
}
criterion_group!(benches, benchmarks);
criterion_main!(benches);
