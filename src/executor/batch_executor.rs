/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::sync::{
    Arc,
    Mutex,
};

use qubit_function::{
    Callable,
    Runnable,
};

use crate::{
    BatchExecutionError,
    BatchOutcome,
};

use super::BatchCallResult;

/// Executes batches of fallible runnable tasks.
///
pub trait BatchExecutor: Send + Sync {
    /// Executes a batch of runnable tasks.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source for the batch. It may be eager or lazy.
    /// * `count` - Declared number of tasks expected from `tasks`.
    ///
    /// # Returns
    ///
    /// `Ok(BatchOutcome)` when the declared task count matches the
    /// source, or `Err(BatchExecutionError)` when the source yields fewer or
    /// more tasks than declared.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when the source task count does not
    /// match `count`.
    ///
    /// # Panics
    ///
    /// Panics from individual tasks are captured in [`BatchOutcome`].
    /// Panics from the configured [`crate::ProgressReporter`] are propagated to
    /// the caller.
    fn execute<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send + std::fmt::Debug;

    /// Executes a batch of callable tasks and collects success values by index.
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
    /// Panics from the configured [`crate::ProgressReporter`] are propagated to
    /// the caller.
    fn call<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = C>,
        C: Callable<R, E> + Send,
        R: Send,
        E: Send + std::fmt::Debug,
    {
        let outputs = Arc::new(
            (0..count)
                .map(|_| Mutex::new(None))
                .collect::<Vec<Mutex<Option<R>>>>(),
        );
        let runnable_tasks = tasks.into_iter().enumerate().map({
            let outputs = Arc::clone(&outputs);
            move |(index, callable)| CallableTask::new(callable, index, Arc::clone(&outputs))
        });
        let outcome = self.execute(runnable_tasks, count)?;
        let values = collect_call_outputs(outputs);
        Ok(BatchCallResult::new(outcome, values))
    }

    /// Applies `action` to every `item` by executing a derived task batch.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source to transform into runnable tasks.
    /// * `count` - Declared number of items expected from `items`.
    /// * `action` - Fallible action applied to each item.
    ///
    /// # Returns
    ///
    /// The result returned by [`Self::execute`] for the derived task batch.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when the source item count does not
    /// match `count`.
    fn for_each<Item, E, I, F>(
        &self,
        items: I,
        count: usize,
        action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = Item>,
        Item: Send,
        F: Fn(Item) -> Result<(), E> + Send + Sync,
        E: Send + std::fmt::Debug,
    {
        let action = Arc::new(action);
        let tasks = items
            .into_iter()
            .map(move |item| ForEachTask::new(item, Arc::clone(&action)));
        self.execute(tasks, count)
    }
}

/// Runnable wrapper used by [`BatchExecutor::for_each`].
struct ForEachTask<Item, E, F>
where
    F: Fn(Item) -> Result<(), E> + Send + Sync,
{
    /// Item consumed by the action exactly once.
    item: Option<Item>,
    /// Shared action applied to each derived task item.
    action: Arc<F>,
}

/// Runnable wrapper used by [`BatchExecutor::call`].
struct CallableTask<C, R> {
    /// Callable consumed and executed exactly once.
    callable: Option<C>,
    /// Zero-based callable index within the batch.
    index: usize,
    /// Shared success-value slots indexed by callable position.
    outputs: Arc<Vec<Mutex<Option<R>>>>,
}

impl<C, R> CallableTask<C, R> {
    /// Creates a runnable wrapper for one callable.
    ///
    /// # Parameters
    ///
    /// * `callable` - Callable to execute.
    /// * `index` - Zero-based callable index within the batch.
    /// * `outputs` - Shared success-value slots.
    ///
    /// # Returns
    ///
    /// A runnable wrapper that stores successful output at `index`.
    fn new(callable: C, index: usize, outputs: Arc<Vec<Mutex<Option<R>>>>) -> Self {
        Self {
            callable: Some(callable),
            index,
            outputs,
        }
    }
}

impl<C, R, E> Runnable<E> for CallableTask<C, R>
where
    C: Callable<R, E>,
{
    /// Executes the wrapped callable and stores its success value.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the callable succeeds, or the callable error when it
    /// fails.
    ///
    /// # Panics
    ///
    /// Panics if this wrapper is run more than once or if an executor runs a
    /// callable whose index is outside the declared batch size.
    fn run(&mut self) -> Result<(), E> {
        let mut callable = self
            .callable
            .take()
            .expect("callable task may only run once");
        let value = callable.call()?;
        let mut slot = self
            .outputs
            .get(self.index)
            .expect("callable index must be within the declared count")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(value);
        Ok(())
    }
}

/// Consumes shared callable output slots into an indexed value vector.
///
/// # Parameters
///
/// * `outputs` - Shared output slots filled by callable wrappers.
///
/// # Returns
///
/// Optional success values indexed by callable position.
///
/// # Panics
///
/// Panics if callable wrappers still hold references to `outputs`.
fn collect_call_outputs<R>(outputs: Arc<Vec<Mutex<Option<R>>>>) -> Vec<Option<R>> {
    let slots = match Arc::try_unwrap(outputs) {
        Ok(slots) => slots,
        Err(_) => panic!("callable output slots should have a single owner after execution"),
    };
    slots
        .into_iter()
        .map(|slot| {
            slot.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        })
        .collect()
}

impl<Item, E, F> ForEachTask<Item, E, F>
where
    F: Fn(Item) -> Result<(), E> + Send + Sync,
{
    /// Creates a runnable wrapper for one `for_each` item.
    ///
    /// # Parameters
    ///
    /// * `item` - Item to pass into `action`.
    /// * `action` - Shared action applied to this item.
    ///
    /// # Returns
    ///
    /// A runnable wrapper that consumes `item` on its first run.
    fn new(item: Item, action: Arc<F>) -> Self {
        Self {
            item: Some(item),
            action,
        }
    }
}

impl<Item, E, F> Runnable<E> for ForEachTask<Item, E, F>
where
    F: Fn(Item) -> Result<(), E> + Send + Sync,
{
    /// Executes the shared action for this derived task item.
    ///
    /// # Returns
    ///
    /// The result returned by the shared action.
    ///
    /// # Panics
    ///
    /// Panics if the derived task is run more than once.
    fn run(&mut self) -> Result<(), E> {
        let item = self.item.take().expect("for_each task may only run once");
        (self.action)(item)
    }
}
