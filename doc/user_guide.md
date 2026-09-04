# Qubit Batch User Guide

[中文用户手册](user_guide.zh_CN.md) · [README](../README.md) ·
[API documentation](https://docs.rs/qubit-batch)

Applies to `qubit-batch` 0.10 and Rust 1.94 or later. This guide is for an
application or library author who has one finite collection to handle now and
needs an auditable outcome, rather than a persistent queue, scheduler, or
worker pool.

## Purpose and Audience

Use `BatchExecutor` when every input represents an independent fallible task
and you need task-level failures, panics, elapsed time, and stable indexes. Use
`BatchProcessor` when a stateful component directly consumes items and reports
how many it completed or processed. The crate consumes the supplied source once
and does not provide retries, durable storage, or a particular async runtime.

## Conceptual Model

```text
finite source ──> executor or processor ──> outcome/result
                   │                         ├─ counters and elapsed time
                   │                         ├─ indexed task failures
                   │                         └─ partial outcome on batch error
                   └─ declared count contract
```

An API such as `for_each` derives its declared count from an
`ExactSizeIterator`. The `*_with_count` forms accept a separate count for a
lazy or externally counted source. A task returning `Err` is recorded in a
normal `BatchOutcome`; a source count mismatch or progress-reporting failure is
a `BatchExecutionError` with the partial outcome attached.

## Scenario: Validate an Import Batch

An import service must validate all received rows, report every bad row, and
retry those rows by their original position. The success criterion is three
attempted rows, two successes, and one failure at index 1.

### Install the crate

```toml
[dependencies]
qubit-batch = "0.10"
```

### Run every validation task

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

`SequentialBatchExecutor` runs in iterator order on the caller thread. The
default policy continues after task errors and captured task panics, so the
service can build a complete retry report from `outcome.failures()`.

## Core Workflow

Choose the entry point from the input you already own:

- `for_each` turns items and a closure into tasks.
- `execute` accepts `qubit-function` `Runnable` tasks.
- `call` accepts `Callable` tasks and returns successful values in
  `BatchCallResult` with their original indexes.
- `process` passes items directly to a `BatchProcessor` implementation.

For an `ExactSizeIterator`, prefer these default methods. They verify the
declared count while consuming the source. When the count is supplied by a
database query or another external boundary, use the corresponding
`*_with_count` method and make the count part of that boundary's contract.

## Advanced Usage

### Use scoped parallel workers deliberately

`ParallelBatchExecutor` uses fixed-width scoped standard threads. Tasks may
borrow from the caller and are complete before the method returns. Its default
configuration handles batches of 100 or fewer declared tasks sequentially to
avoid worker setup overhead. Configure a builder only after measuring your
representative workload:

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

let outcome = executor
    .for_each(0..8, |value| {
        assert!(value < 8);
        Ok::<(), &'static str>(())
    })
    .expect("range length should be exact");

assert!(outcome.is_success());
```

`sequential_threshold(0)` requests scoped workers for every non-empty batch.
It does not create a reusable thread pool. For Rayon-backed execution, use the
companion `qubit-rayon-batch` crate.

### Stop after task failures

The default `TaskFailurePolicy::Continue` collects all task failures. Configure
`StopOnFirstFailure` or `StopAfterFailures(...)` through an executor builder
when further source items should not be accepted after the threshold. In this
case an `Ok(BatchOutcome)` can describe early termination; inspect
`outcome.termination()` before treating the source count as fully validated.

## Errors and Diagnostics

Inspect the result in two layers:

1. On `Ok(BatchOutcome)`, inspect `is_success()`, counters, and `failures()`.
   A captured task panic is a `BatchTaskError::Panicked`; a returned task error
   is `BatchTaskError::Failed`.
2. On `Err(BatchExecutionError)`, inspect `outcome()` before handling the
   error. `CountShortfall` and `CountExceeded` preserve the declared and
   observed counts; progress reporting and incomplete scheduling also retain
   the work already accounted for.

For `call`, `BatchCallError` additionally preserves sparse successful
`BatchCallOutput` entries collected before the batch-level error. Do not use a
task failure as a signal to retry the entire batch automatically: the crate
does not know whether a task's side effects are idempotent.

## Troubleshooting

| Symptom | Check | Action |
| --- | --- | --- |
| `Ok` result has failures | `outcome.is_success()` and `failures()` | Handle each indexed task error or panic. |
| `CountShortfall` | The external declared count and source producer | Correct the producer/count boundary; use `outcome()` for completed work. |
| Small batches do not use workers | `sequential_threshold()` | Lower the threshold only after benchmarking. |
| A task panic is not returned | Whether the panic came from a task body | Task-body panics are captured; progress-reporter and processor-consumer panics can propagate. |

## Limitations and Best Practices

- Supply a finite source and do not rely on this crate for durable scheduling,
  retry orchestration, or persistence.
- Treat the declared count as a contract. Use `*_with_count` only when the
  count genuinely comes from a separate source.
- Parallel execution creates scoped workers per call and waits for accepted
  tasks before returning; choose thread count and threshold from measurements.
- A progress interval is a throttle at implementation-defined progress points,
  not a guarantee of an immediate running event.
- Keep retry decisions outside the crate and make them safe for the effects of
  the task being retried.

## Further Reading

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-batch)
- [Crate package](https://crates.io/crates/qubit-batch)
