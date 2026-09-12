# Batch execution design

[中文设计说明](design.zh_CN.md) · [User guide](user_guide.md) · [README](../README.md)

This document describes qubit-batch 0.12. Rust 1.94 and edition 2024 remain the
minimum toolchain contract. It records the runtime and result invariants used
by the standard-thread implementation and the qubit-rayon-batch companion.

## Ownership and boundaries

Executors model independent fallible tasks. Processors model a component that
consumes batches, possibly delegating chunks to a database or another service.
They retain different error and result types because a chunk delegate can have
partial external effects that are not representable as independent task errors.
Neither layer owns retries, transactions, persistent queues, or async runtimes.

The sequential executor runs on the caller thread. The standard parallel
executor creates scoped workers per call; Rayon owns a reusable dedicated pool.
Both parallel implementations use ParallelBatchExecutionCoordinator for
progress lifecycle, task accounting, and final validation. Their scheduler
closures own source consumption, bounded dispatch, and joining/draining work.

Private task adapters and state live in execute/internal and process/internal.
utils/internal holds the indexed processor work envelope. Internal modules do
not add public hooks for tests. ProgressFailure::fail_operation shares only the
identical failed-terminal delivery mapping; each result owner builds its own
partial outcome and determines its primary error.

## Admission and completion

```text
source.next() -> observed -> accepted token -> started -> succeeded / failed
                     |             |                       |
                  overflow       drain                counters + failures
```

A token carries its execution identity and unique source index. It cannot be
constructed publicly or cloned. execute_task consumes it once and rejects a
token belonging to another execution. The coordinator checks that all accepted
tokens completed before accepting a successful scheduler return.

next_task is the preferred source API. A source None records exhaustion; an
admission None can instead mean failure-policy stop, progress failure, or count
overflow. The coordinator result is authoritative. accept_task remains a lower
level adapter and does not observe exhaustion on behalf of its caller.

Failure policies stop admission cooperatively. Already accepted tokens drain,
so the final failure count may exceed the configured threshold. Concurrent
admission can overlap a failure; no exact last accepted index is promised.
A policy check before pulling avoids consuming another source item after an
already observed stop. It cannot cancel an iterator next call already running.

Only an actually observed source None proves exhaustion. If exhaustion was
observed before a worker failure, a short source remains CountShortfall rather
than a policy stop. Finished on an Ok outcome describes absence of a policy
short circuit, not universal task success. On an error, inspect the enclosing
error as well; the attached outcome is partial accounting.

## Error precedence and progress

Start failure prevents scheduling. After the scheduler returns, precedence is:

1. Scheduler rejection, retaining any secondary automatic/terminal reporter error.
2. Automatic reporter failure, retaining all completed task accounting.
3. Accepted but unfinished tokens (IncompleteSchedule).
4. Observed count overflow (CountExceeded).
5. Failure-policy stop while exhaustion is unobserved.
6. Exhausted/returned short source (CountShortfall).
7. A source-aware scheduler that returned without proving source exhaustion
   (IncompleteSchedule; failed terminal delivery is retained as `report_error`).
8. Terminal progress delivery and the final outcome.

Synchronous reporter, iterator, and task-destructor panics may unwind the call.
Task-body unwinds are captured as failures; aborting panics cannot be caught.
Automatic reporter errors and caught reporter panics become ProgressFailure.
Consumers in processors do not have executor task-panic capture semantics.

A positive progress interval throttles running events; it is not a deadline.
Sequential progress points are between items and chunk progress points are
between completed chunks. Parallel reporting uses a scoped reporter thread.
Zero interval disables throttling and uses completion notifications without
busy spinning; notifications may be coalesced. Callbacks must avoid dependency
cycles with workers or the thread waiting for batch completion.

## Results and chunk boundaries

BatchOutcome satisfies completed = succeeded + failed + panicked, completed <=
task_count, one failure per failed/panicked task, and unique in-range indexes.
Failures are sorted by index. BatchCallResult stores sparse successful outputs
with strictly increasing indexes, disjoint from failure indexes, and output
count equal to succeeded_count. Its cross-check is linear in successes plus
failures; successful output collection may require a separate sort.

BatchCallError retains outputs from that call only. An application performing
multiple call invocations must retain earlier outputs and add its own global
index offset. Task failures still appear in Ok(BatchCallResult).

BatchProcessResult satisfies processed <= completed <= item_count. Processed
counts successful input items, not affected database rows. Chunked processing
requires an Ok delegate result to report the submitted item count and full
completion. A lower processed count is allowed. On delegate error or invalid
success, the outer result includes only preceding successful chunks; the
excluded chunk may already have changed external state. The delegate error
retains whatever additional detail the delegate chooses to provide.

## Validation and migration from 0.10

Outcome validation no longer allocates a HashSet for failure indexes. It checks
aggregate counters, then scans for out-of-range indexes, sorts in place, checks
adjacent duplicates, and validates failure variants. Sorting takes O(F log F)
worst-case time without another heap allocation for F failure records.

This intentionally changes which error is returned for multiply-invalid input:
all range errors precede duplicate errors, and duplicates report the smallest
repeated index. The first range error remains the first in input order. Counter
error ordering and callable-result error ordering are unchanged. Callers should
update tests that relied on the old duplicate traversal order. The in-place
validation change was introduced in qubit-batch 0.11. qubit-batch 0.12 retains
that error ordering. The current companion is qubit-rayon-batch 0.10, which
depends on qubit-batch 0.12.

## Resource model and testing

Standard workers and their bounded queue scale with configured worker count;
the current implementation bounds unfinished source observations by 2W + 1.
This is a tested backend property, not a universal custom-scheduler promise.
Failures retain O(F) data; callable results retain O(S + F) data. Explicit
application chunks bound retained outputs but do not implement global policies.
The default sequential threshold stays 100; benchmark the actual workload.

Tests cover public behavior in root tests. Production admission Loom models
live in src/tests and require RUSTFLAGS=--cfg loom plus the loom-tests feature.
The standalone boolean model is removed. The Rayon repository applies shared
generic assertions to sequential, standard parallel, Rayon, and fallback paths;
backend-specific tests cover bounded admission, draining and reentrancy.

Both README files and both guides are compiled directly as doctest documents.
Correctness checks belong outside benchmark timing. End-to-end concurrent
benchmarks include caller thread creation; steady-callers benchmarks reuse
caller threads but include request/result channel handoff. Performance results
must name the workload and environment; no universal speedup is promised.

[Performance measurements](performance.md)

## Callable fallback and chunk storage

`execute::spi::call_with_executor` owns the generic sparse-output adapter and
calls `execute_with_count`, not the overridden callable method. A single outer
iterator defers user `into_iter` until executor admission starts. Standard and
Rayon callable overrides use direct sequential Vec collection for fallback;
Rayon briefly releases its path-selection guard before the helper re-enters
`execute_with_count`, with no user code executed in that interval.
`BatchCallError::map_scheduler_error` preserves owned outputs and secondary
reporter errors without Clone bounds.

Chunk delegation uses `Vec::drain(..)` so the wrapper retains buffer capacity.
Dropping a partially consumed Drain releases its remaining items. Delegate error
and invalid-success aggregates still exclude the current chunk; no source
consumption audit or destruction-order guarantee is added.

Policy-stop labels reflect source observation, not completion count. A final
failed task can leave all declared tasks complete without a source None; a
parallel source can instead exhaust before that failure. Tests deliberately
exercise both orders using explicit channel handshakes.
