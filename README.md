# Qubit Batch

[![Rust CI](https://github.com/qubit-ltd/rs-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-batch/coverage-badge.json)](https://qubit-ltd.github.io/rs-batch/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-batch` executes a finite batch once and returns structured accounting for
the caller that needs to validate, import, or maintain many independent items
without coupling a shared library to a particular async runtime.

## Installation

```toml
[dependencies]
qubit-batch = "0.11"
```

Use Rust 1.94 or later. Add `qubit-function` only when implementing
`Runnable`, `Callable`, or `Consumer` types directly, and add `qubit-progress`
only when implementing a custom progress reporter.

## Quick Start

An import service receives three records and wants to retain the invalid
record's stable index for a retry report while still checking every record.

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportError {
    record_id: u64,
}

let records = [
    (101, "alice@example.com"),
    (102, "not-an-email"),
    (103, "carol@example.com"),
];

let outcome = SequentialBatchExecutor::new()
    .for_each(records, |(record_id, email)| {
        if email.contains('@') {
            Ok(())
        } else {
            Err(ImportError { record_id })
        }
    })
    .expect("array length should be exact");

assert_eq!(outcome.task_count(), 3);
assert_eq!(outcome.succeeded_count(), 2);
assert_eq!(outcome.failed_count(), 1);
assert_eq!(outcome.failures()[0].index(), 1);
assert!(matches!(outcome.failures()[0].error(), BatchTaskError::Failed(_)));
```

The call succeeds because the source count matches the array length; task
failures remain in `BatchOutcome` for inspection.

## Behavioral Boundaries

`ParallelBatchExecutor` creates scoped workers for one call and uses the
sequential executor for batches at or below `sequential_threshold`. The Rayon
companion also falls back to sequential execution when a batch re-enters the
same Rayon pool, so nested work does not wait for the pool's own workers. Each
nested call still returns its own outcome; failures are not merged into the
outer call automatically.

The concrete sequential callable APIs accept callable values that are
inherently `FnMut`; a callable closure does not need to implement `Fn`. The
parallel `BatchExecutor` trait keeps its `Send` bounds because accepted work
may run on scoped workers.

For processors, `processed_count` is the number of successfully processed
input items and satisfies `processed_count <= completed_count <= item_count`.
Business measurements such as affected database rows must be tracked
separately. Callable result validation stores successful values and ordered
failures in O(S + F) space, where S is the successful count and F is the
failure count.

When a chunk delegate fails, its error exposes the result for the preceding
successful chunks only. The failed chunk may already have produced external
side effects, so retry boundaries and idempotency remain the caller's
responsibility. For a large logical input, process independent chunks and
decide in the outer loop whether to continue or retry; the pattern does not
provide a global failure policy, global stable indexes, or cross-chunk
automatic retry. Add the chunk offset to a local output index when a global
index is required.

## Why This Project Exists

Many applications need one bounded operation, not a queue or an always-on
worker pool. This crate centralizes count validation, elapsed-time accounting,
stable failure indexes, captured task panics, and partial outcomes, while
leaving retry policy and runtime ownership to the application.

## What It Provides

- `BatchExecutor` for fallible tasks and `BatchProcessor` for direct item
  processing.
- `SequentialBatchExecutor` and `ParallelBatchExecutor` for caller-thread and
  scoped parallel task execution.
- Sequential execution on the caller thread, plus fixed-width scoped standard
  threads for larger parallel batches.
- `BatchOutcome` and `BatchProcessResult` counters, indexed failures, and
  partial outcomes attached to batch-level errors.
- Explicit count contracts through `*_with_count` APIs and configurable task
  failure policies.

It is not a queue, scheduler, persistent worker pool, retry framework, or a
Rayon adapter. The companion `qubit-rayon-batch` crate supplies Rayon-backed
execution. Parallel workers are scoped to one call; benchmark representative
workloads before changing the default sequential fallback threshold.

## Learn More

- [User guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [Design](doc/design.md)
- [中文设计说明](doc/design.zh_CN.md)
- [API documentation](https://docs.rs/qubit-batch)
- [Crate package](https://crates.io/crates/qubit-batch)
- [中文 README](README.zh_CN.md)

## Source and termination contract

Runtime-specific parallel schedulers should use
`ParallelBatchExecutionContext::next_task` to pull and admit source items. A
`None` returned by the source is recorded as exhaustion; a `None` returned
before that can mean progress failure, a task-failure-policy stop, or an
observation beyond the declared count. Inspect the coordinator result to
distinguish them. `accept_task` is a lower-level adapter and does not record
source exhaustion; prefer `next_task` so the coordinator can distinguish an
exhausted source from a policy stop.

Accepted task tokens are drained before an execution returns. A failure policy
stops future admission and does not cancel already accepted work. A source that
is observed exhausted still receives count-shortfall validation before a task
failure policy is used. `Finished` means source consumption was not stopped by
the policy, and does not mean that every task succeeded; inspect
`BatchOutcome::is_success()` and `BatchOutcome::failures()`.

Callable results retain successful values and ordered failures. Their indexed
cross-check is linear in the number of successes and failures, while output
collection and failure sorting have their own costs. Processor results count
successful input items; domain measurements such as affected database rows
belong in application state. A failed chunk may already have produced external
side effects, so retry and idempotency remain the caller's responsibility.

Callable small-batch and single-worker fallbacks collect outputs directly on
the caller thread; Rayon same-pool reentry uses that path too. A policy stop can
still have `completed_count == task_count`, while `Finished` can include task
failures. Use counters and failure indexes alongside termination when deciding
what to retry; see the [user guide](doc/user_guide.md).

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)
