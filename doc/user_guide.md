# Qubit Batch User Guide

[中文用户手册](user_guide.zh_CN.md) · [README](../README.md) ·
[API documentation](https://docs.rs/qubit-batch)

Applies to `qubit-batch` 0.12 and Rust 1.94 or later. This guide is for an
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

## Installation and Minimal Configuration

```toml
[dependencies]
qubit-batch = "0.12"
```

## Core Workflow

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

Choose the entry point from the input you already own:

- `for_each` turns items and a closure into tasks.
- `execute` accepts `qubit-function` `Runnable` tasks.
- `call` accepts `Callable` tasks and returns successful values in
  `BatchCallResult` with their original indexes.
- `process` passes items directly to a `BatchProcessor` implementation.

For an `ExactSizeIterator`, prefer these methods to derive the declared count.
Built-in implementations validate it while consuming the source; a custom
`BatchProcessor` defines its own count-validation behavior. When the count is supplied by a
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

### Use a processor directly

Use `SequentialBatchProcessor` when a stateful consumer owns the batch
operation and should run on the caller thread. `with_consumer` stores a
consumer directly, so it can borrow caller-owned state and need not be `Send`:

```rust
use qubit_batch::{BatchProcessor, SequentialBatchProcessor};

let prefix = String::from("ok");
let mut processor = SequentialBatchProcessor::with_consumer(|item: &String| {
    assert!(item.starts_with(&prefix));
});
let result = processor
    .process([String::from("okay"), String::from("ok")])
    .expect("array length should be exact");
assert!(result.is_success());
```

Use `ParallelBatchProcessor` when the consumer is `Send + Sync` and the work
should be shared by scoped workers. It uses sequential execution for small
batches by default; configure `thread_count` and `sequential_threshold` after
measuring the workload:

```rust
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use qubit_batch::{BatchProcessor, ParallelBatchProcessor};

let total = Arc::new(AtomicUsize::new(0));
let consumer_total = Arc::clone(&total);
let mut processor = ParallelBatchProcessor::builder(move |item: &usize| {
    consumer_total.fetch_add(*item, Ordering::Relaxed);
})
.thread_count(2)
.sequential_threshold(0)
.build()
.expect("parallel processor configuration should be valid");
let result = processor.process([1, 2, 3]).expect("array length should be exact");
assert!(result.is_success());
assert_eq!(total.load(Ordering::Relaxed), 6);
```

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
the `offset` above is the caller-owned mapping to a global index. Each `call`
returns only its own outcome. If that call has a batch-level error, its
`BatchCallError` retains successful outputs from the current chunk, not from
preceding chunks. Retain earlier results in application state. Task failures
remain in an `Ok(BatchCallResult)` and must be inspected separately.

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

`sequential_threshold(0)` requests scoped workers for non-empty batches when
more than one worker is configured.
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

### Recover outputs from the current call

The application has already saved two outputs. A count error in the next
chunk preserves that call's successful values; the application adds its own
offset and retains the earlier values. Validate the upstream count before
retrying, and do not replay successful side effects blindly.

```rust
use qubit_batch::SequentialBatchExecutor;

let executor = SequentialBatchExecutor::new();
let mut saved = vec![(0usize, 10usize), (1, 20)];
let offset = 2usize;
let error = executor.call_with_count(
    [30usize, 40].into_iter().map(|value| move || Ok::<_, &'static str>(value)),
    3,
).expect_err("the current source is shorter than declared");
assert!(error.source().is_count_shortfall());
assert_eq!(error.outcome().completed_count(), 2);
for output in error.into_outputs() {
    let (local_index, value) = output.into_parts();
    saved.push((offset + local_index, value));
}
assert_eq!(saved, [(0, 10), (1, 20), (2, 30), (3, 40)]);
```

### Configure a failure limit

This sequential example stops exactly at the second failure. Parallel workers
may finish additional accepted tasks; the limit controls admission, not the
final failure count.

```rust
use std::num::NonZeroUsize;
use qubit_batch::BatchTermination;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;

let executor = SequentialBatchExecutor::builder()
    .task_failure_policy(TaskFailurePolicy::StopAfterFailures(
        NonZeroUsize::new(2).expect("failure limit is positive"),
    ))
    .build();
let result = executor.call((0..10).map(|index| move || {
    if index % 2 == 0 { Ok(index) } else { Err("invalid record") }
})).expect("policy stops are normal outcomes");
assert_eq!(result.outcome().termination(), BatchTermination::StoppedByTaskFailurePolicy);
assert_eq!(result.outcome().failed_count(), 2);
assert_eq!(result.outputs().len(), 2);
```

### Record progress events

Add the progress dependency for this reporter and the scheduler example above.
Callbacks must not wait for work that itself waits for the reporter. Running
events may be coalesced on parallel paths; only lifecycle ordering is asserted.

```toml
[dependencies]
qubit-batch = "0.12"
qubit-progress = { version = "0.8", default-features = false }
```

```rust
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use qubit_batch::SequentialBatchExecutor;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

#[derive(Default)]
struct AuditReporter(Mutex<Vec<Phase>>);
impl Reporter for AuditReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        self.0.lock().expect("audit log lock must be healthy").push(event.phase());
        Ok(())
    }
}
let reporter = Arc::new(AuditReporter::default());
let executor = SequentialBatchExecutor::builder()
    .reporter_arc(reporter.clone())
    .report_interval(Duration::ZERO)
    .build();
let result = executor.call([|| Ok::<_, &'static str>(42)])
    .expect("the batch and reporter must succeed");
assert!(result.outcome().is_success());
let phases = reporter.0.lock().expect("audit log lock must be healthy");
assert_eq!(phases.first(), Some(&Phase::Started));
assert_eq!(phases.last(), Some(&Phase::Succeeded));
```

### Delegate bounded database chunks

The example validates each chunk before writing. A real database delegate must
supply transaction or idempotency guarantees: the aggregate excludes a failed
chunk even if its delegate has already written some rows.

```rust
use std::num::NonZeroUsize;
use std::time::Duration;
use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessError;
use qubit_batch::ChunkedBatchProcessor;

struct InsertRows;
impl BatchProcessor<u64> for InsertRows {
    type Error = &'static str;
    fn process_with_count<I>(&mut self, rows: I, count: usize)
        -> Result<BatchProcessResult, Self::Error>
    where I: IntoIterator<Item = u64> {
        let rows: Vec<_> = rows.into_iter().collect();
        if rows.len() != count { return Err("wrong row count"); }
        if rows.contains(&0) { return Err("invalid row"); }
        BatchProcessResult::builder(count)
            .completed_count(count).processed_count(count)
            .chunk_count(usize::from(count > 0)).elapsed(Duration::ZERO)
            .build().map_err(|_| "invalid result")
    }
}
let mut processor = ChunkedBatchProcessor::builder(
    InsertRows, NonZeroUsize::new(2).expect("chunk size is positive"),
).build();
let error = processor.process([1, 2, 0, 4, 5])
    .expect_err("the second chunk contains an invalid row");
match error {
    ChunkedBatchProcessError::ChunkFailed {
        chunk_index, start_index, chunk_len, source, result, ..
    } => {
        assert_eq!((chunk_index, start_index, chunk_len), (1, 2, 2));
        assert_eq!(source, "invalid row");
        assert_eq!(result.processed_count(), 2);
        assert_eq!(result.chunk_count(), 1);
    }
    other => panic!("unexpected chunk failure: {other:?}"),
}
```

## Completion counts and termination

A final declared task can fail after every declared task has completed but
before the source returns `None`. The sequential executor stops immediately;
it does not pull another item just to prove exhaustion:

```rust
use qubit_batch::{BatchTermination, SequentialBatchExecutor, TaskFailurePolicy};
let executor = SequentialBatchExecutor::builder()
    .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure).build();
let outcome = executor.execute_with_count([|| Err::<(), _>("invalid")], 1)
    .expect("policy stop returns an outcome");
assert_eq!(outcome.completed_count(), 1);
assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
assert!(!outcome.is_success());
```

If a parallel producer observes `None` before its worker fails, the same input
can instead yield `Finished` (or `CountShortfall` when the source is short).
A small-batch or same-pool fallback can therefore change the termination label
without changing the task results. `Finished` is not a promise of task success,
and `completed_count == task_count` does not prove the source count was validated
when the failure policy stopped admission. Inspect the enclosing batch error,
completion counters, and indexed failures together before choosing retry work.
Already accepted parallel tasks drain, so the failure count can exceed the limit.

Callable fallback uses the sequential executor's direct output collection for
small batches or a single worker; Rayon also uses it for same-pool reentry.
The trait's `Send` requirements and sparse, ordered result contract still apply.
The generic parallel adapter defers custom `IntoIterator::into_iter` execution
until its executor consumes the source inside its scheduling boundary. This
guarantee applies to the explicit-count adapter; `call` first converts its
exact-size source to obtain `len()` before dispatch, as before.


Chunked processing reuses its input buffer. Delegates receive an iterator, not
a promised Vec representation or destruction order. A failed chunk can have
external effects; its aggregate still includes only earlier successful chunks.

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
already caused external side effects. Chunk metadata locates the failed input,
but does not prove a safe replay boundary; use the delegate's transaction or
idempotency contract to decide whether and how to retry.

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

- [Design and migration](design.md)
- [Performance measurements](performance.md)

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-batch)
- [Crate package](https://crates.io/crates/qubit-batch)
