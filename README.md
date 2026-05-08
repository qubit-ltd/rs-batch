# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

One-shot batch execution and processing utilities for the Qubit Rust libraries.

## What it does

Use `qubit-batch` when you already have a finite batch and want to run it once
with consistent accounting:

- attempt every record in an import, validation, or maintenance job;
- keep stable zero-based failure indexes for diagnostics and retries;
- collect completed, succeeded, failed, and panicked task counts;
- detect producer bugs when an iterator yields fewer or more items than
  declared;
- avoid binding shared library code to Tokio, Rayon, or another runtime.

This crate is not a queue, scheduler, worker pool, or retry framework. It
consumes the supplied iterator once and returns a structured result.

## Core model

- `BatchExecutor` runs fallible tasks. Use `for_each` for item-oriented jobs,
  `execute` for explicit `Runnable` tasks, and `call` for `Callable` tasks that
  return values.
- `BatchOutcome` is the executor result. It reports task counters, elapsed time,
  and indexed `BatchTaskFailure` entries.
- `BatchExecutionError` is a batch contract error. It means the iterator count
  did not match the declared count, and it carries the partial `BatchOutcome`.
- `SequentialBatchExecutor` runs tasks in iterator order on the caller thread.
- `ParallelBatchExecutor` runs tasks on fixed-width scoped standard threads.
- `BatchProcessor` processes data items directly instead of wrapping them as
  tasks.
- `SequentialBatchProcessor` and `ParallelBatchProcessor` invoke a
  `qubit-function` `Consumer` per item and support progress reporting.
- `ChunkedBatchProcessor` splits one logical batch into fixed-size chunks and
  delegates each chunk to another `BatchProcessor`. A delegate that returns
  `Ok` for a chunk must report `item_count == chunk_len` and
  `completed_count == chunk_len`; `processed_count` may be lower when the
  underlying operation reports fewer successful or affected rows.

Rayon-backed execution lives in the companion `qubit-rayon-batch` crate.

## Installation

```toml
[dependencies]
qubit-batch = "0.4.5"
```

Add `qubit-function` when you implement `Runnable`, `Callable`, or `Consumer`
types directly, and add `qubit-progress` when you implement custom progress
reporters.

## Examples

### Validate every item

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportError {
    record_id: u64,
    reason: &'static str,
}

let executor = SequentialBatchExecutor::new();
let records = [
    (101, "alice@example.com"),
    (102, "not-an-email"),
    (103, "carol@example.com"),
];

let result = executor
    .for_each(records, 3, |(record_id, email)| {
        if email.contains('@') {
            Ok(())
        } else {
            Err(ImportError {
                record_id,
                reason: "email address is invalid",
            })
        }
    })
    .expect("the iterator yielded exactly the declared number of records");

assert_eq!(result.task_count(), 3);
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);

let failure = &result.failures()[0];
assert_eq!(failure.index(), 1);
match failure.error() {
    BatchTaskError::Failed(error) => {
        assert_eq!(error.record_id, 102);
        assert_eq!(error.reason, "email address is invalid");
    }
    BatchTaskError::Panicked { .. } => unreachable!("the closure returned an error"),
}
```

### Run in parallel

```rust
use qubit_batch::{
    BatchExecutor,
    ParallelBatchExecutor,
};

let executor = ParallelBatchExecutor::builder()
    .thread_count(4)
    .sequential_threshold(0)
    .build()
    .expect("parallel executor configuration should be valid");

let result = executor
    .for_each(0..8, 8, |value| {
        assert!(value < 8);
        Ok::<(), &'static str>(())
    })
    .expect("range length should match the declared count");

assert!(result.is_success());
```

`ParallelBatchExecutor::default()` keeps batches with 100 or fewer declared
tasks on the sequential executor to avoid scoped-thread setup overhead. Set
`sequential_threshold(0)` when every non-empty batch should use parallel
workers.

### Collect callable values

```rust
use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};

fn count_users() -> Result<usize, &'static str> {
    Ok(3)
}
fn count_orders() -> Result<usize, &'static str> {
    Ok(5)
}

let result = SequentialBatchExecutor::new()
    .call([count_users, count_orders], 2)
    .expect("callable count should match");

assert!(result.outcome().is_success());
assert_eq!(result.values(), &[Some(3), Some(5)]);
```

### Process items directly

```rust
use qubit_batch::{
    BatchProcessor,
    SequentialBatchProcessor,
};

let mut processor = SequentialBatchProcessor::new(|item: &i32| {
    assert!(*item > 0);
});

let result = processor
    .process([1, 2, 3], 3)
    .expect("the iterator yielded exactly three items");

assert_eq!(result.completed_count(), 3);
assert_eq!(result.processed_count(), 3);
```

### Delegate fixed-size chunks

```rust
use std::{
    num::NonZeroUsize,
    time::Duration,
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessResultBuilder,
    BatchProcessor,
    ChunkedBatchProcessor,
};

struct InsertChunk;

impl BatchProcessor<i32> for InsertChunk {
    type Error = &'static str;

    fn process<I>(&mut self, rows: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let processed = rows.into_iter().count();
        BatchProcessResultBuilder::builder(count)
            .completed_count(processed)
            .processed_count(processed)
            .chunk_count(1)
            .elapsed(Duration::ZERO)
            .build()
            .map_err(|_| "invalid process result")
    }
}

let mut processor = ChunkedBatchProcessor::new(
    InsertChunk,
    NonZeroUsize::new(2).expect("chunk size is non-zero"),
);

let result = processor
    .process([1, 2, 3, 4, 5], 5)
    .expect("the iterator yielded exactly five items");

assert_eq!(result.completed_count(), 5);
assert_eq!(result.processed_count(), 5);
assert_eq!(result.chunk_count(), 3);
```

When `ChunkedBatchProcessor` delegates a chunk, the delegate result is treated
as the result for that exact submitted chunk. Returning `Ok` means the delegate
has reached a terminal outcome for every item in the chunk, so `item_count` and
`completed_count` must both match the submitted chunk length. `processed_count`
can be lower than the chunk length for domains where the target reports a
smaller success count, such as an idempotent database insert that accepts three
rows but affects only two. If the delegate cannot reach a terminal outcome for
the whole chunk, it should return `Err`; inconsistent `Ok` results are reported
as `ChunkedBatchProcessError::InvalidChunkResult`.

## Progress Reporting

`qubit-batch` accepts `qubit-progress` reporters but does not re-export
`qubit-progress` types. Implement reporters from `qubit-progress` directly.
`SequentialBatchExecutor`, `ParallelBatchExecutor`, `SequentialBatchProcessor`,
`ParallelBatchProcessor`, and `ChunkedBatchProcessor` can all attach custom
reporters.

```rust
use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};
use qubit_progress::{
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
};

struct ConsoleReporter;

impl ProgressReporter for ConsoleReporter {
    fn report(&self, event: &ProgressEvent) {
        let counters = event.counters();
        let total = counters.total_count().unwrap_or(counters.completed_count());
        match event.phase() {
            ProgressPhase::Started => println!("starting {total} tasks"),
            ProgressPhase::Running => println!(
                "completed {}/{total}, active {}, elapsed {:?}",
                counters.completed_count(),
                counters.active_count(),
                event.elapsed(),
            ),
            ProgressPhase::Finished => println!("finished {total} tasks in {:?}", event.elapsed()),
            ProgressPhase::Failed | ProgressPhase::Canceled => println!(
                "stopped after {}/{total} tasks in {:?}",
                counters.completed_count(),
                event.elapsed(),
            ),
        }
    }
}

let executor = SequentialBatchExecutor::new()
    .with_reporter(ConsoleReporter)
    .with_report_interval(Duration::from_millis(250));

let result = executor
    .for_each(["a", "b", "c"], 3, |_item| Ok::<(), &'static str>(()))
    .expect("item count should match");

assert!(result.is_success());
```

Panics from task bodies are captured as `BatchTaskError::Panicked`. Panics from
processor consumers and progress reporters propagate to the caller because they
are outside the task failure model. Sequential execution and processing report
progress only between tasks or items; parallel variants report running progress
periodically from a scoped reporter thread.

The configured `report_interval` is a throttle checked only at
implementation-defined running progress points. It does not guarantee that a
running event is emitted immediately when the interval elapses. Sequential
variants check between tasks or items, and chunked processing checks after a
chunk completes. Parallel variants use a scoped reporter thread; with a positive
interval they can also emit periodic running events while workers are active.
`Duration::ZERO` disables time throttling, so running progress is reported as
soon as each implementation-defined progress point is reached; it does not
create a tight refresh loop.

## Count Contract

Execution and processing APIs require a declared count. This lets the API report
stable totals before consuming lazy iterators and return partial results when a
producer yields the wrong number of items.

```rust
use qubit_batch::{
    BatchExecutionError,
    BatchExecutor,
    SequentialBatchExecutor,
};

let executor = SequentialBatchExecutor::new();
let error = executor
    .for_each([10, 20], 3, |_value| Ok::<(), &'static str>(()))
    .expect_err("the iterator yielded fewer items than declared");

match error {
    BatchExecutionError::CountShortfall {
        expected,
        actual,
        outcome,
    } => {
        assert_eq!(expected, 3);
        assert_eq!(actual, 2);
        assert_eq!(outcome.completed_count(), 2);
    }
    BatchExecutionError::CountExceeded { .. } => unreachable!(),
}
```

Important result semantics:

- `Ok(BatchOutcome)` does not mean every task succeeded. It means the
  supplied iterator matched the declared count.
- `result.is_success()` means all declared tasks completed without task errors
  or panics.
- `Err(BatchExecutionError)` means the iterator produced fewer or more items
  than declared and carries a partial `BatchOutcome`.

## API Cheat Sheet

- `SequentialBatchExecutor::new()` runs tasks deterministically on the caller
  thread in iterator order.
- `ParallelBatchExecutor::default()` uses available CPU parallelism, scoped
  standard threads, and a sequential fallback for batches with 100 or fewer
  declared tasks. Use `ParallelBatchExecutor::builder().sequential_threshold(0)`
  to force parallel workers for every non-empty batch.
- `BatchOutcome::failures()` returns failure records sorted by zero-based task
  index.
- `BatchCallResult::values()` stores `Some(value)` only for successful
  callables; failed and panicked callables have `None`.
- `BatchProcessResult::processed_count()` is the delegate-reported success
  count. It can differ from `completed_count()` for processors that report
  affected rows or similar target-side counts.
- `ChunkedBatchProcessError<E>` carries the partial aggregate result for count
  mismatches and delegate failures.

## Project Layout

- `src/execute`: batch execution traits, outcomes, count mismatch errors, task
  failures, and execution adapters.
- `src/execute/impls`: standard-library batch executor implementations.
- `src/process`: data-item batch processor traits, results, and processing
  errors.
- `src/process/impls`: consumer-backed processors and the chunked processor.
- `src/utils`: crate-internal utilities shared by execution and processing.
- `tests/execute`: behavior tests for batch execution, progress callbacks,
  failures, panics, outcomes, and count mismatches.
- `tests/process`: behavior tests for direct processing, chunking, delegate
  errors, and progress callbacks.
- `tests/utils`: behavior tests for shared internal utility behavior.
- `tests/docs`: README consistency checks.

## Documentation

- API documentation: [docs.rs/qubit-batch](https://docs.rs/qubit-batch)
- Crate package: [crates.io/crates/qubit-batch](https://crates.io/crates/qubit-batch)
- Source repository: [github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)

## Testing and CI

Run the fast local checks from the crate root:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

To match the repository CI environment, run:

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

`./align-ci.sh` aligns the local toolchain and CI-related configuration before
`./ci-check.sh` runs the same checks used by the pipeline. Use `./coverage.sh`
when changing behavior that should be reflected in coverage reports.

## Contributing

Issues and pull requests are welcome. Please keep changes focused, add or update
tests when behavior changes, and update this README or rustdoc when public API
or user-visible behavior changes.

By contributing, you agree that your contribution is licensed under the same
[Apache License, Version 2.0](LICENSE) as this project.

## License and Copyright

Copyright © 2026 Haixing Hu, Qubit Co. Ltd.

This software is licensed under the [Apache License, Version 2.0](LICENSE).

## Author and Maintenance

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **Repository** | [github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch) |
| **API documentation** | [docs.rs/qubit-batch](https://docs.rs/qubit-batch) |
| **Crate** | [crates.io/crates/qubit-batch](https://crates.io/crates/qubit-batch) |
