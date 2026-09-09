// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use qubit_function::Callable;

use crate::BatchCallError;
use crate::BatchCallResult;
use crate::BatchExecutor;
use crate::execute::batch_executor::collect_call_outputs;
use crate::execute::internal::CallableTask;

/// Collects indexed callable outputs through an executor's runnable entry
/// point.
///
/// Runtime adapters can use this function for their parallel path while
/// overriding `call_with_count` to collect sequential outputs directly. This
/// function calls `execute_with_count`, never `call_with_count`, so an override
/// can delegate here without recursion. Source conversion and consumption are
/// delayed until the executor pulls work inside its scheduling boundary.
///
/// # Parameters
///
/// * `executor` - Executor that completes all accepted tasks before returning.
/// * `tasks` - Possibly borrowed, lazy callable source; the source need not be
///   Send.
/// * `count` - Exact declared callable count, not a capacity hint.
///
/// # Type Parameters
///
/// * `X` - Batch executor type.
/// * `C` - Callable task type.
/// * `R` - Callable success value type.
/// * `E` - Callable error type.
/// * `I` - Callable source type.
///
/// # Returns
///
/// Successful values sorted by source index with their execution outcome.
///
/// # Errors
///
/// Returns a batch-level error retaining all successful outputs on count,
/// progress, or scheduler failure. Task errors remain in the outcome.
///
/// # Panics
///
/// Propagates iterator and synchronous reporter panics. Task-body panics are
/// captured by the executor. Panics if the executor violates its synchronous
/// ownership or output-count contract. Outputs occupy O(S) space for S
/// successes.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::SequentialBatchExecutor;
/// use qubit_batch::execute::spi::call_with_executor;
/// let result = call_with_executor(
///     &SequentialBatchExecutor::new(), [|| Ok::<_, ()>(42)], 1,
/// ).expect("exact source");
/// assert_eq!(*result.outputs()[0].value(), 42);
/// ```
pub fn call_with_executor<X, C, R, E, I>(
    executor: &X,
    tasks: I,
    count: usize,
) -> Result<BatchCallResult<R, E>, BatchCallError<R, E, X::SchedulerError>>
where
    X: BatchExecutor + ?Sized,
    I: IntoIterator<Item = C>,
    C: Callable<R, E> + Send,
    R: Send,
    E: Send,
{
    let outputs = Arc::new(SegQueue::new());
    // Even IntoIterator::into_iter may invoke user code. Keep it inside the
    // executor's admission/reentrancy boundary, not just Iterator::next.
    let runnable_tasks = std::iter::once(tasks)
        .flat_map(IntoIterator::into_iter)
        .enumerate()
        .map({
            let outputs = Arc::clone(&outputs);
            move |(index, callable)| CallableTask::new(callable, index, Arc::clone(&outputs))
        });
    let execution = executor.execute_with_count(runnable_tasks, count);
    let outputs = collect_call_outputs(outputs);
    match execution {
        Ok(outcome) => Ok(BatchCallResult::try_new(outcome, outputs)
            .expect("call output collection must match successful task indexes and counts")),
        Err(source) => Err(BatchCallError::new(source, outputs)),
    }
}
