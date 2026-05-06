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

/// Runnable wrapper used by [`crate::executor::BatchExecutor::call`].
pub(crate) struct CallableTask<C, R> {
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
    pub(crate) fn new(callable: C, index: usize, outputs: Arc<Vec<Mutex<Option<R>>>>) -> Self {
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
