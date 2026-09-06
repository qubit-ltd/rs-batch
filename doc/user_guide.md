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

### Callable and processor contracts

The concrete callable methods on `SequentialBatchExecutor` accept callable
values through their `FnMut` behavior. A closure that mutates captured state
does not need to implement `Fn`. The `BatchExecutor` trait keeps `Send` bounds
for parallel implementations because accepted tasks and returned values may
cross scoped worker threads.

For a `BatchProcessor`, `processed_count` counts successful input items and
must satisfy `processed_count <= completed_count <= item_count`. A domain
measurement such as affected database rows is separate state owned by the
processor; it must not be stored in `processed_count` when it can exceed the
number of inputs.

### Process a large source in explicit chunks

Use the existing callable API when the source is too large for one in-memory
result. Each iteration below owns an independent outcome, while the outer loop
decides whether to continue after a failure:

```rust
use qubit_batch::{BatchExecutor, SequentialBatchExecutor};

let executor = SequentialBatchExecutor::new();
let mut source = 0..10_000usize;
let mut offset = 0usize;
let mut sum = 0usize;

loop {
    let chunk: Vec<_> = source.by_ref().take(256).collect();
    if chunk.is_empty() {
        break;
    }
    let chunk_len = chunk.len();
    let result = executor
        .call(chunk.into_iter().map(|item| move || Ok::<_, ()>(item)))
        .expect("chunk length should be exact");
    assert!(result.outcome().is_success());
    for output in result.into_outputs() {
        let global_index = offset + output.index();
        let _ = global_index;
        sum += *output.value();
    }
    offset += chunk_len;
}

assert_eq!(sum, (0..10_000usize).sum());
```

This pattern does not provide a global failure policy, global stable indexes,
or automatic retry across chunks. Callable indexes are local to each result;
the `offset` above is the caller-owned mapping to a global index. A failed
chunk exposes only the successful prefix from preceding chunks, and retrying
the failed boundary remains an explicit, idempotency-aware caller decision.

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
companion `qubit-rayon-batch` crate. Its same-pool nested calls fall back to
sequential execution on the worker that made the nested call. This avoids
waiting for the same pool's workers; arbitrary task dependencies or cross-pool
cycles still require an application-level design.

### Stop after task failures

The default `TaskFailurePolicy::Continue` collects all task failures. Configure
`StopOnFirstFailure` or `StopAfterFailures(...)` through an executor builder
when further source items should not be accepted after the threshold. In this
case an `Ok(BatchOutcome)` can describe early termination; inspect
`outcome.termination()` before treating the source count as fully validated.

Runtime-specific schedulers should use `next_task` when they own a lazy source,
so a source `None` is recorded separately from an admission stop:

```rust
use std::{convert::Infallible, sync::Arc, time::Duration};
use qubit_batch::{BatchExecutor, TaskFailurePolicy};
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::NoopReporter;

let coordinator = ParallelBatchExecutionCoordinator::new(
    Arc::new(NoopReporter), Duration::ZERO);
let tasks = [|| Ok::<(), &'static str>(())];
let outcome = coordinator.execute(tasks, 1, TaskFailurePolicy::Continue,
    |tasks, context| {
        let mut source = tasks.into_iter();
        while let Some(token) = context.next_task(&mut source) {
            context.execute_task(token);
        }
        Ok::<(), Infallible>(())
    }).unwrap();
assert!(outcome.is_success());
```

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
`BatchCallOutput` entries collected before the batch-level error. A successful
`BatchCallResult` retains S successful values and F ordered failures, so its
validation and retained-result space is O(S + F). Do not use a task failure as
a signal to retry the entire batch automatically: the crate does not know
whether a task's side effects are idempotent.

For `ChunkedBatchProcessor`, an error's attached result includes only chunks
that completed successfully before the failed chunk. The failed chunk may have
already caused external side effects, so the chunk is the retry boundary and
the caller must decide whether and how to retry it.

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
