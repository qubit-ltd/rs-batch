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
