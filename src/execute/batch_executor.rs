// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use qubit_function::Callable;
use qubit_function::Runnable;

use super::BatchCallError;
use super::BatchCallOutput;
use super::BatchCallResult;
use super::callable_task::CallableTask;
use super::for_each_task::ForEachTask;
use crate::BatchExecutionError;
use crate::BatchOutcome;

/// Executes batches of fallible tasks.
///
/// Implementations consume the supplied iterator once, execute every observed
/// task unless an explicitly declared count is exceeded or an explicitly
/// configured task-failure policy stops execution, and return a
/// [`BatchOutcome`] containing task-level successes, failures, panics, and
/// elapsed time.
///
/// # Implementor contract
///
/// An implementation must execute every task it accepts before returning and
/// must not retain or execute task values after the method returns. It must
/// stop accepting tasks once `count` has been exceeded and report the observed
/// count through [`BatchExecutionError::CountExceeded`]. The default
/// [`Self::call`] and [`Self::call_with_count`] adapters rely on these
/// synchronous ownership and count guarantees when collecting callable
/// outputs.
///
/// # When to use an executor
///
/// Choose an executor when each input item is an independent runnable or
/// callable operation and the important result is per-task success, failure,
/// panic, and timing metadata. Executors are a good fit for heterogeneous
/// work, retry classification, and parallel CPU/I/O tasks where the caller
/// needs stable failure indexes. Use [`BatchExecutor::for_each`] when the
/// input values are merely a convenient way to create those independent
/// tasks.
///
/// Choose a [`crate::BatchProcessor`] when a component owns a stateful,
/// batch-level consumer (for example, a DAO writer) and the primary contract
/// is how many items/chunks that component accepted and processed. A processor
/// may deliberately batch items, mutate internal state, or expose a domain
/// error instead of one task failure per item. The two APIs therefore overlap
/// in mechanics but represent different ownership and result semantics.
///
/// ```rust
/// use qubit_batch::{
///     BatchExecutor,
///     SequentialBatchExecutor,
/// };
///
/// let outcome = SequentialBatchExecutor::new()
///     .for_each([1, 2, 3], |value| {
///         assert!(value > 0);
///         Ok::<(), &'static str>(())
///     })
///     .expect("array length should be exact");
///
/// assert!(outcome.is_success());
/// ```
pub trait BatchExecutor: Send + Sync {
    /// Error returned when the runtime scheduler cannot submit work.
    type SchedulerError: Error + Send + Sync + 'static;
    /// Executes a batch of runnable tasks whose iterator exposes an exact
    /// length.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source for the batch. Its iterator must report the
    ///   remaining task count exactly.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::execute_with_count`] after deriving the
    /// declared count from the iterator length.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress reporting
    /// fails, or a count-mismatch variant when the iterator violates its exact
    /// length contract while being consumed.
    ///
    /// # Panics
    ///
    /// Panics from individual tasks are captured in [`BatchOutcome`]. Reporter
    /// callbacks invoked synchronously may panic; automatic reporter failures
    /// are returned as [`BatchExecutionError::ProgressReport`].
    /// Implementations must not return while an accepted task can still run.
    fn execute<T, E, I>(&self, tasks: I) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
        T: Runnable<E> + Send,
        E: Send,
    {
        let tasks = tasks.into_iter();
        let count = tasks.len();
        self.execute_with_count(tasks, count)
    }

    /// Executes a batch of runnable tasks with an explicit declared count.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source for the batch. It may be eager or lazy.
    /// * `count` - Declared number of tasks expected from `tasks`.
    ///
    /// # Returns
    ///
    /// `Ok(BatchOutcome)` when the declared task count matches the source, or
    /// when a configured task-failure policy stops consumption before the
    /// remaining source count can be validated. Inspect
    /// [`BatchOutcome::termination`] to distinguish the latter case.
    /// `Err(BatchExecutionError)` when progress reporting fails or when a fully
    /// consumed source yields fewer or more tasks than declared.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress reporting
    /// fails, a count-mismatch variant when a fully consumed source task count
    /// does not match `count`, or [`BatchExecutionError::IncompleteSchedule`]
    /// when a parallel scheduler accepts a task without completing it.
    ///
    /// # Panics
    ///
    /// Panics from individual tasks are captured in [`BatchOutcome`]. Reporter
    /// callbacks invoked synchronously may panic; automatic reporter failures
    /// are returned as [`BatchExecutionError::ProgressReport`].
    /// Implementations must not return while an accepted task can still run.
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send;

    /// Executes callable tasks whose iterator exposes an exact length.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Callable task source for the batch. Its iterator must report
    ///   the remaining callable count exactly.
    ///
    /// # Returns
    ///
    /// A [`BatchCallResult`] containing the normal execution summary plus
    /// optional success values indexed by callable position.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallError`] when progress reporting fails, the iterator
    /// violates its exact length contract, or a parallel scheduler leaves an
    /// accepted callable incomplete. The error preserves values returned by
    /// callables that completed before execution stopped.
    ///
    /// # Panics
    ///
    /// Panics from individual callables are captured in the execution result.
    /// Reporter callbacks invoked synchronously may panic; automatic reporter
    /// failures are returned as [`BatchExecutionError::ProgressReport`].
    /// Implementations must not return while an accepted callable can still
    /// run; the adapter collects outputs immediately after this method
    /// returns.
    fn call<C, R, E, I>(&self, tasks: I) -> Result<BatchCallResult<R, E>, BatchCallError<R, E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = C>,
        I::IntoIter: ExactSizeIterator,
        C: Callable<R, E> + Send,
        R: Send,
        E: Send,
    {
        let tasks = tasks.into_iter();
        let count = tasks.len();
        self.call_with_count(tasks, count)
    }

    /// Executes callable tasks with an explicit declared count and collects
    /// success values by index.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Callable task source for the batch.
    /// * `count` - Declared number of callables expected from `tasks`.
    ///
    /// # Returns
    ///
    /// A [`BatchCallResult`] containing the normal execution summary plus
    /// optional success values indexed by callable position.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallError`] when progress reporting fails or when the
    /// source callable count does not match `count`. The error preserves values
    /// returned by callables that completed before execution stopped.
    ///
    /// # Panics
    ///
    /// Panics from individual callables are captured in the execution result.
    /// Reporter callbacks invoked synchronously may panic; automatic reporter
    /// failures are returned as [`BatchExecutionError::ProgressReport`].
    /// Implementations must not return while an accepted callable can still
    /// run; the adapter collects outputs immediately after this method
    /// returns.
    fn call_with_count<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchCallError<R, E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = C>,
        C: Callable<R, E> + Send,
        R: Send,
        E: Send,
    {
        let outputs = Arc::new(SegQueue::new());
        // This adapter is lazy: callables are wrapped as runnable tasks only
        // when the executor consumes the iterator. The callables themselves are
        // still executed later by `CallableTask::run`.
        let runnable_tasks = tasks.into_iter().enumerate().map({
            let outputs = Arc::clone(&outputs);
            move |(index, callable)| CallableTask::new(callable, index, Arc::clone(&outputs))
        });
        let execution = self.execute_with_count(runnable_tasks, count);
        let outputs = collect_call_outputs(outputs);
        match execution {
            Ok(outcome) => Ok(BatchCallResult::try_new(outcome, outputs)
                .expect("call output collection must return one value slot per declared task")),
            Err(source) => Err(BatchCallError::new(source, outputs)),
        }
    }

    /// Applies `action` to every item whose iterator exposes an exact length.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source to transform into runnable tasks.
    /// * `action` - Fallible action applied to each item.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::for_each_with_count`] after deriving the
    /// declared count from the iterator length.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress reporting
    /// fails, or a count-mismatch variant when the iterator violates its exact
    /// length contract while being consumed.
    ///
    /// # Panics
    ///
    /// Propagates panics raised by `action` or synchronous reporter callbacks.
    fn for_each<Item, E, I, F>(
        &self,
        items: I,
        action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = Item>,
        I::IntoIter: ExactSizeIterator,
        Item: Send,
        F: Fn(Item) -> Result<(), E> + Send + Sync,
        E: Send,
    {
        let items = items.into_iter();
        let count = items.len();
        self.for_each_with_count(items, count, action)
    }

    /// Applies `action` to every item using an explicit declared count.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source to transform into runnable tasks.
    /// * `count` - Declared number of items expected from `items`.
    /// * `action` - Fallible action applied to each item.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::execute_with_count`] for the derived task
    /// batch.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress reporting
    /// fails, or a count-mismatch variant when the source item count does not
    /// match `count`.
    ///
    /// # Panics
    ///
    /// Propagates panics raised by `action` or synchronous reporter callbacks.
    fn for_each_with_count<Item, E, I, F>(
        &self,
        items: I,
        count: usize,
        action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = Item>,
        Item: Send,
        F: Fn(Item) -> Result<(), E> + Send + Sync,
        E: Send,
    {
        let action = Arc::new(action);
        let tasks = items
            .into_iter()
            .map(move |item| ForEachTask::new(item, Arc::clone(&action)));
        self.execute_with_count(tasks, count)
    }
}

/// Consumes shared callable outputs into sorted sparse outputs.
///
/// # Parameters
///
/// * `outputs` - Shared output queue filled by callable wrappers.
/// # Returns
///
/// Successful outputs sorted by callable index.
///
/// # Panics
///
/// Panics if callable wrappers still hold references to `outputs`.
pub(crate) fn collect_call_outputs<R>(outputs: Arc<SegQueue<(usize, R)>>) -> Vec<BatchCallOutput<R>> {
    let outputs = match Arc::try_unwrap(outputs) {
        Ok(outputs) => outputs,
        Err(_) => panic!("callable output queue should have a single owner after execution"),
    };
    let mut collected = Vec::new();
    while let Some((index, value)) = outputs.pop() {
        collected.push(BatchCallOutput::new(index, value));
    }
    collected.sort_unstable_by_key(BatchCallOutput::index);
    collected
}
