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

use qubit_function::Runnable;

/// Runnable wrapper used by [`crate::executor::BatchExecutor::for_each`].
pub(crate) struct ForEachTask<Item, E, F>
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
    pub(crate) fn new(item: Item, action: Arc<F>) -> Self {
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
