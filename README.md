# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Batch-oriented task execution abstractions and sequential utilities for the
Qubit Rust libraries.

## When to use this crate

Use `qubit-batch` when you already have a finite set of fallible tasks and want
to run the whole set as one batch with consistent accounting:

- data import or validation jobs where every record should be attempted;
- maintenance scripts that need a final success/failure summary;
- pipelines that want stable task indexes for diagnostics and retries;
- shared library code that should not commit to Tokio, Rayon, or another
  concrete runtime.

This crate is not a queue, scheduler, worker pool, or retry framework. It
executes the supplied batch once and returns a structured result.

## Overview

Qubit Batch focuses on one-shot execution of whole task batches instead of
single-task submission. The crate provides:

- `BatchExecutor`: trait for executing a batch of fallible runnable tasks.
- `BatchExecutor::call`: convenience API for executing fallible callables and
  collecting successful return values by index.
- `BatchProcessor`: trait for processing data items directly, without first
  wrapping each item as a task.
- `ChunkedBatchProcessor`: processor adapter that submits fixed-size chunks to a
  delegate processor.
- `SequentialBatchExecutor`: deterministic, in-order execution on the caller
  thread.
- `ParallelBatchExecutor`: fixed-width parallel execution backed by scoped
  standard threads.
- `ProgressReporter`: `qubit-progress` reporter trait receiving structured
  `ProgressEvent` lifecycle notifications.
- `WriterProgressReporter` and `LoggerProgressReporter`: concrete reporters
  re-exported from `qubit-progress` for writers and the `log` crate.
- `BatchOutcome`: structured batch outcome with failure aggregation and
  monotonic elapsed-duration reporting.

Rayon-backed parallel batch execution lives in the companion
`qubit-rayon-batch` crate.

## Features

- Accept eager or lazy task sources through `IntoIterator`.
- Keep batch-level statistics for completed, succeeded, failed, and panicked
  tasks.
- Record per-task failures with stable batch indexes and readable panic
  messages.
- Treat task failures as batch data instead of short-circuiting the whole
  execution.
- Detect declared task-count mismatches and return the partial result collected
  before the mismatch was observed.
- Keep the core API free of runtime-specific dependencies while still offering
  a standard-library parallel executor.

## Installation

```toml
[dependencies]
qubit-batch = "0.4.0"
```

## Quick Start

### Process items with `for_each`

For item-oriented jobs, `for_each` is usually the smallest API surface. The
closure is converted into runnable tasks internally, and every item is attempted.

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
assert_eq!(result.completed_count(), 3);
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);
assert!(!result.is_success());

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

### Execute explicit tasks

Use `execute` when your tasks already implement `Runnable` or when you want to
build task values before running the batch.

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};
use qubit_function::Runnable;

#[derive(Debug)]
enum FileTask {
    Validate(&'static str),
    Upload(&'static str),
    Cleanup(&'static str),
}

impl Runnable<&'static str> for FileTask {
    fn run(&mut self) -> Result<(), &'static str> {
        match self {
            Self::Validate(path) if path.ends_with(".csv") => Ok(()),
            Self::Validate(_) => Err("unsupported file type"),
            Self::Upload(_) => Ok(()),
            Self::Cleanup(path) if *path == "/tmp/protected" => {
                panic!("cleanup path is protected");
            }
            Self::Cleanup(_) => Ok(()),
        }
    }
}

let tasks = vec![
    FileTask::Validate("customers.csv"),
    FileTask::Upload("customers.csv"),
    FileTask::Validate("notes.txt"),
    FileTask::Cleanup("/tmp/protected"),
];

let executor = SequentialBatchExecutor::new();
let result = executor
    .execute(tasks, 4)
    .expect("the iterator yielded exactly four tasks");

assert_eq!(result.completed_count(), 4);
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);
assert_eq!(result.panicked_count(), 1);

for failure in result.failures() {
    match failure.error() {
        BatchTaskError::Failed(error) => {
            println!("task #{} failed: {error}", failure.index());
        }
        BatchTaskError::Panicked { message } => {
            println!("task #{} panicked: {message:?}", failure.index());
        }
    }
}
```

If you implement `Runnable` in a downstream crate, add `qubit-function` as a
dependency too:

```toml
[dependencies]
qubit-batch = "0.4.0"
qubit-function = "0.11"
```

### Process data in fixed-size chunks

Implement `BatchProcessor` for the real batch target, such as a SQL insert
operation. Wrap it with `ChunkedBatchProcessor` when the logical batch must be
submitted in smaller chunks.

```rust
use std::{
    num::NonZeroUsize,
    time::Duration,
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessor,
};

struct InsertRows;

impl BatchProcessor<i32> for InsertRows {
    type Error = &'static str;

    fn process<I>(&mut self, rows: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let processed = rows.into_iter().count();
        Ok(BatchProcessResult::new(
            count,
            processed,
            processed,
            1,
            Duration::ZERO,
        ))
    }
}

let mut processor = ChunkedBatchProcessor::new(
    InsertRows,
    NonZeroUsize::new(2).expect("chunk size is non-zero"),
);

let result = processor
    .process([1, 2, 3, 4, 5], 5)
    .expect("the iterator yielded exactly five items");

assert_eq!(result.completed_count(), 5);
assert_eq!(result.processed_count(), 5);
assert_eq!(result.chunk_count(), 3);
```

## Progress Reporting

`SequentialBatchExecutor` uses `NoOpProgressReporter` by default. You can attach
your own reporter and tune the minimum interval between in-flight callbacks.
Sequential execution emits progress only between tasks, so a single long-running
task will not produce intermediate running callbacks.

```rust
use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
    SequentialBatchExecutor,
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
the reporter itself are not captured as task failures; they propagate to the
caller because progress reporting is outside the task failure model.
Progress events contain only phase, counters, optional stage, and elapsed time;
use logging, metrics, or tracing for non-progress observability.

## Count Contract

Both `execute` and `for_each` require the caller to pass the declared item count.
This lets the executor report consistent totals before consuming the iterator and
also detect producer bugs:

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
- `result.is_success()` is the convenience check for “all declared tasks
  completed without task errors or panics.”
- `Err(BatchExecutionError)` means the iterator produced fewer or more items
  than declared. The error still carries a partial `BatchOutcome`.

## API Notes

- `SequentialBatchExecutor::new()` is deterministic and runs tasks on the caller
  thread in iterator order.
- `ParallelBatchExecutor::default()` uses available CPU parallelism and scoped
  standard threads, so tasks may borrow data from the caller.
- `BatchOutcome::failures()` returns failure records sorted by task
  index.
- `BatchTaskFailure::index()` is zero-based and refers to the task's position in
  the batch.
- The core crate intentionally avoids runtime dependencies. Use
  `ParallelBatchExecutor` for a standard-library fixed-width executor, or the
  companion `qubit-rayon-batch` crate when you need Rayon-backed CPU execution.

## Public API Cheat Sheet

- `BatchExecutor`: trait for executing a declared batch of fallible runnable
  tasks.
- `BatchCallResult<R, E>`: callable batch result containing the execution
  summary and indexed success values.
- `SequentialBatchExecutor`: default executor that runs tasks sequentially on the
  caller thread.
- `ParallelBatchExecutor`: fixed-width scoped-thread executor for parallel
  batch execution without a runtime dependency.
- `BatchProcessor`: trait for processing a declared batch of data items.
- `BatchProcessResult`: aggregate result containing item, processed, chunk, and
  monotonic elapsed-duration counters.
- `ChunkedBatchProcessor`: processor wrapper that splits a logical batch into
  fixed-size chunks and delegates each chunk.
- `ChunkedBatchProcessError<E>`: chunked processor error for source count
  mismatches or delegate failures, carrying the partial process result.
- `ProgressReporter`: `qubit-progress` trait receiving `ProgressEvent` values
  for batch start, periodic progress, and terminal notifications.
- `NoOpProgressReporter`: default reporter that accepts callbacks without doing
  any work.
- `WriterProgressReporter` and `LoggerProgressReporter`: concrete reporters
  re-exported from `qubit-progress` for writers and `log`.
- `BatchOutcome<E>`: aggregate result containing task counts, monotonic
  elapsed duration, and detailed task failures.
- `BatchExecutionError<E>`: batch-level contract error for declared count
  shortfall or overflow, carrying the partial result collected so far.
- `BatchTaskFailure<E>`: one failed or panicked task with its stable zero-based
  batch index.
- `BatchTaskError<E>`: task-level failure classified as either a returned
  business error or a captured panic.

## Project Layout

- `src/executor`: executor traits and the sequential executor implementation.
- `src/error`: batch execution results, count mismatch errors, task failures,
  and task panic conversion.
- `src/processor`: data-item batch processor traits, results, and the chunked
  processor.
- `src/progress`: re-exported progress event and reporter types from
  `qubit-progress`.
- `tests/executor`: behavior tests for sequential execution, progress callbacks,
  failures, panics, and count mismatches.
- `tests/processor`: behavior tests for chunking, delegate errors, and progress
  callbacks.
- `tests/progress`: behavior tests for concrete progress reporters.
- `tests/error`: tests for result invariants and error helper methods.
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
