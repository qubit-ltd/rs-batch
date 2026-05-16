/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use qubit_function::{
    Callable,
    Runnable,
};

use crate::{
    BatchExecutionError,
    BatchOutcome,
};

use super::{
    BatchCallResult,
    callable_task::CallableTask,
    for_each_task::ForEachTask,
};

/// Executes batches of fallible tasks.
///
/// Implementations consume the supplied iterator once, execute every observed
/// task unless an explicitly declared count is exceeded, and return a
/// [`BatchOutcome`] containing task-level successes, failures, panics, and
/// elapsed time.
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
    /// Returns [`BatchExecutionError`] only if the iterator violates its exact
    /// length contract while being consumed.
    ///
    /// # Panics
    ///
    /// Panics from individual tasks are captured in [`BatchOutcome`].
    /// Panics from the configured
    /// [`qubit_progress::reporter::ProgressReporter`] are propagated to the
    /// caller.
    fn execute<T, E, I>(&self, tasks: I) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
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
    /// `Err(BatchExecutionError)` when the source yields fewer or more tasks
    /// than declared.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when the source task count does not
    /// match `count`.
    ///
    /// # Panics
    ///
    /// Panics from individual tasks are captured in [`BatchOutcome`].
    /// Panics from the configured
    /// [`qubit_progress::reporter::ProgressReporter`] are propagated to the
    /// caller.
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
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
    /// Returns [`BatchExecutionError`] only if the iterator violates its exact
    /// length contract while being consumed.
    ///
    /// # Panics
    ///
    /// Panics from individual callables are captured in the execution result.
    /// Panics from the configured
    /// [`qubit_progress::reporter::ProgressReporter`] are propagated to the
    /// caller.
    fn call<C, R, E, I>(&self, tasks: I) -> Result<BatchCallResult<R, E>, BatchExecutionError<E>>
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
    /// Returns [`BatchExecutionError`] when the source callable count does not
    /// match `count`.
    ///
    /// # Panics
    ///
    /// Panics from individual callables are captured in the execution result.
    /// Panics from the configured
    /// [`qubit_progress::reporter::ProgressReporter`] are propagated to the
    /// caller.
    fn call_with_count<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchExecutionError<E>>
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
        let outcome = self.execute_with_count(runnable_tasks, count)?;
        let values = collect_call_outputs(outputs, count);
        Ok(BatchCallResult::new(outcome, values))
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
    /// Returns [`BatchExecutionError`] only if the iterator violates its exact
    /// length contract while being consumed.
    fn for_each<Item, E, I, F>(
        &self,
        items: I,
        action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
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
    /// Returns [`BatchExecutionError`] when the source item count does not
    /// match `count`.
    fn for_each_with_count<Item, E, I, F>(
        &self,
        items: I,
        count: usize,
        action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
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

/// Consumes shared callable outputs into an indexed value vector.
///
/// # Parameters
///
/// * `outputs` - Shared output queue filled by callable wrappers.
/// * `count` - Declared callable count used to size the result vector.
///
/// # Returns
///
/// Optional success values indexed by callable position.
///
/// # Panics
///
/// Panics if callable wrappers still hold references to `outputs`, or if a
/// queued output index is outside the declared batch size.
fn collect_call_outputs<R>(outputs: Arc<SegQueue<(usize, R)>>, count: usize) -> Vec<Option<R>> {
    let outputs = match Arc::try_unwrap(outputs) {
        Ok(outputs) => outputs,
        Err(_) => panic!("callable output queue should have a single owner after execution"),
    };
    let mut values = Vec::with_capacity(count);
    values.resize_with(count, || None);
    while let Some((index, value)) = outputs.pop() {
        let slot = values
            .get_mut(index)
            .expect("callable index must be within the declared count");
        *slot = Some(value);
    }
    values
}
