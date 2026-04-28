/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::sync::Arc;

use qubit_function::Runnable;

use crate::{
    BatchExecutionError,
    BatchExecutionResult,
};

/// Executes batches of fallible runnable tasks.
///
/// # Author
///
/// Haixing Hu
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
    /// `Ok(BatchExecutionResult)` when the declared task count matches the
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
    /// Panics from individual tasks are captured in [`BatchExecutionResult`].
    /// Panics from the configured [`crate::ProgressReporter`] are propagated to
    /// the caller.
    fn execute<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchExecutionResult<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send;

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
    ) -> Result<BatchExecutionResult<E>, BatchExecutionError<E>>
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
