# Performance measurements

[中文](performance.zh_CN.md) · [Design](design.md)

## Failure-index validation

Measured on 2026-09-07 with Rust 1.94.0, release profile, Linux x86_64,
and an Intel Core i5-9600K (six cores). The baseline is commit `2067172`
(qubit-batch 0.10.0); the candidate is the 0.11.0 in-place sorting implementation.
Input consists of F failed tasks with unique indexes in reverse order. Input
vector construction and destruction are outside the measured build interval.
A single-threaded process wraps the system allocator and counts allocation and
reallocation requests, resetting counters immediately before `build()`.

Each size was measured three times; all repeats gave the same values:

| F | Baseline allocations | Baseline requested bytes | Candidate allocations | Candidate requested bytes |
|---:|---:|---:|---:|---:|
| 1,000 | 1 | 18,448 | 0 | 0 |
| 10,000 | 1 | 147,472 | 0 | 0 |
| 100,000 | 1 | 1,179,664 | 0 | 0 |

These are extra allocation requests during validation, not total result memory
or resident memory. Results still own an O(F) failure vector. The reproducible
allocation reduction justifies removing the auxiliary HashSet. Error precedence
changes are documented in [the migration section](design.md#validation-and-migration-from-010).

Criterion timing was inconclusive: the 100,000-failure baseline point estimate
was 2.0654 ms; candidate runs gave 0.3395 ms and 3.63 ms. This uncontrolled
machine did not establish a repeatable speedup. Do not use those timings to
select a parallel threshold or claim throughput improvements.

## Reproduction

Create a temporary Cargo binary with edition 2024 and dependencies named `old`
and `new`, both with `package = "qubit-batch"`, pointing to the baseline and
candidate checkouts. Place both beside the same rs-progress checkout so their
relative path dependency resolves to one package. Run `cargo run --release`.
The complete allocation probe is below; it deliberately runs without worker
threads so unrelated allocations cannot race counter resets.

```rust,ignore
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
struct AllocationProbe;
unsafe impl GlobalAlloc for AllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: AllocationProbe = AllocationProbe;

fn main() {
    for count in [1000usize, 10_000, 100_000] {
        for repeat in 0..3 {
            let failures = (0..count).rev().map(|index| old::BatchTaskFailure::new(index, old::BatchTaskError::Failed(()))).collect();
            let builder = old::BatchOutcomeBuilder::builder(count).completed_count(count).failed_count(count).failures(failures);
            CALLS.store(0, Ordering::Relaxed); BYTES.store(0, Ordering::Relaxed);
            let result = std::hint::black_box(builder.build().unwrap());
            let old_calls = CALLS.load(Ordering::Relaxed); let old_bytes = BYTES.load(Ordering::Relaxed);
            assert_eq!(result.failed_count(), count);
            drop(result);
            let failures = (0..count).rev().map(|index| new::BatchTaskFailure::new(index, new::BatchTaskError::Failed(()))).collect();
            let builder = new::BatchOutcomeBuilder::builder(count).completed_count(count).failed_count(count).failures(failures);
            CALLS.store(0, Ordering::Relaxed); BYTES.store(0, Ordering::Relaxed);
            let result = std::hint::black_box(builder.build().unwrap());
            let new_calls = CALLS.load(Ordering::Relaxed); let new_bytes = BYTES.load(Ordering::Relaxed);
            assert_eq!(result.failed_count(), count);
            println!("count={count} repeat={repeat} old_allocations={old_calls} old_bytes={old_bytes} new_allocations={new_calls} new_bytes={new_bytes}");
        }
    }
}
```

## Scheduling workloads

`cargo bench --bench scheduling_matrix_bench` compares sequential and scoped
executors. The plain groups measure one call. `end-to-end-concurrent-*` includes
caller thread creation and joins. `steady-callers-*` creates caller threads
before timing and reuses them; request/result channel handoff remains timed.
Both perform untimed validation before measuring. Standard executor worker
creation still occurs inside every batch in all groups. Run `cargo bench
--bench scheduling_matrix_bench -- --test` for a correctness smoke check.
These workloads do not measure Rayon; its benchmarks live in rs-rayon-batch.

## Callable fallback measurements (2026-09-09)

Environment: Intel Core i5-9600K, six cores, Linux x86_64 7.0.0-30-generic,
Rust 1.94.0 / LLVM 21.1.8, release profile, NoopReporter. Baselines are
qubit-batch `62e2a3d` and qubit-rayon-batch `99ce8c5`, with the same new
benchmark sources overlaid. Path dependency qubit-progress is 0.8.3 in both.
The candidate adds direct sequential callable collection and reusable chunks;
the production Rayon shared queue is unchanged.

`cargo bench --locked --bench call_fallback_bench` covers 0/1/32/100/101/1024
items, scalar/String outputs, two workers with threshold 100 or 0, and one
worker with threshold 0. Executors are constructed outside timing; standard
scoped workers are still created inside each parallel call. Each point uses
20 samples, 200 ms warmup, and 500 ms requested measurement. Untimed checks
validate success and output counts. Result destruction is included.

Selected means and 95% confidence intervals, in microseconds:

| Configuration / items | Baseline mean [95% CI] | Refined mean [95% CI] |
| --- | ---: | ---: |
| default / 32 | 4.013 [3.971, 4.062] | 2.520 [2.507, 2.533] |
| default / 100 | 11.293 [11.199, 11.401] | 7.052 [7.008, 7.095] |
| one-worker / 1024 | 108.982 [107.303, 111.160] | 62.852 [62.440, 63.312] |
| parallel / 1024 | 2086.781 [2045.877, 2141.154] | 2073.002 [2055.272, 2093.523] |

These are sequentially collected local measurements, not a portable speedup
promise. Worker creation and system scheduling make tiny forced-parallel calls
especially noisy. Do not change the default threshold of 100 from these data.
Criterion raw estimates and sample data are emitted under `target/criterion`.
Use the same benchmark source and dependency checkout for baseline comparison.

## Chunk buffer allocation requests

A single-threaded global allocator probe counted allocation/reallocation
requests only during `process_with_count`, using a consuming u64 delegate and
NoopReporter. Input ranges and processor construction are outside measurement.
All three repeats returned identical counts. These are total requests during
the call, not resident memory or Vec-specific counts.

| Items | Chunk size | Old calls | Old bytes | New calls | New bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1024 | 1 | 1031 | 33220 | 8 | 484 |
| 1024 | 16 | 197 | 14716 | 8 | 604 |
| 1024 | 256 | 29 | 14716 | 8 | 2524 |
| 1024 | 4096 | 8 | 8668 | 8 | 8668 |
| 65536 | 1 | 65543 | 2097604 | 8 | 484 |
| 65536 | 16 | 12293 | 917884 | 8 | 604 |
| 65536 | 256 | 1793 | 1038844 | 8 | 2524 |
| 65536 | 4096 | 173 | 1015804 | 8 | 33244 |

`cargo bench --locked --bench chunk_buffer_bench` measures the same sizes with
20 samples, 200 ms warmup and 500 ms requested measurement. Buffer reuse removes
repeated chunk growth; it does not make progress setup allocation-free.

Reproduce the allocation experiment with the probe below in a standalone Cargo
binary depending on the checkout under measurement, then `cargo run --release`.
Do not run other allocation-producing threads in the probe process.

```rust,ignore
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use qubit_batch::{BatchProcessResult, BatchProcessor, ChunkedBatchProcessor};
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
struct Probe;
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) { unsafe { System.dealloc(pointer, layout) } }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(size, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[global_allocator] static ALLOCATOR: Probe = Probe;
struct Consume;
impl BatchProcessor<u64> for Consume {
    type Error = std::convert::Infallible;
    fn process_with_count<I: IntoIterator<Item=u64>>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error> {
        for item in items { black_box(item); }
        Ok(BatchProcessResult::builder(count).completed_count(count).processed_count(count).chunk_count(1).build().unwrap())
    }
}
fn main() {
    for count in [1024usize, 65536] {
        for chunk_size in [1usize, 16, 256, 4096] {
            for repeat in 0..3 {
                let mut processor = ChunkedBatchProcessor::new(Consume, NonZeroUsize::new(chunk_size).unwrap());
                CALLS.store(0, Ordering::Relaxed); BYTES.store(0, Ordering::Relaxed);
                let result = processor.process_with_count(0..count as u64, count).unwrap();
                let calls = CALLS.load(Ordering::Relaxed); let bytes = BYTES.load(Ordering::Relaxed);
                assert_eq!(result.completed_count(), count);
                println!("{count},{chunk_size},{repeat},{calls},{bytes}");
            }
        }
    }
}

```

### Tiny forced-parallel follow-up

The initial single-item forced-parallel point was noisy. Three additional
interleaved old/new runs (reversing order in round two) gave these means in
microseconds. The initial large regression did not reproduce.

| Round | Old | New | New / old |
| ---: | ---: | ---: | ---: |
| 1 | 33.766 | 31.062 | 0.920 |
| 2 | 31.067 | 31.832 | 1.025 |
| 3 | 33.698 | 30.774 | 0.913 |
