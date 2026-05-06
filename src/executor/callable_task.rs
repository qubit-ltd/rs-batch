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

/// Runnable wrapper used by [`crate::executor::BatchExecutor::call`].
pub(crate) struct CallableTask<C, R> {
    /// Callable consumed and executed exactly once.
    callable: Option<C>,
    /// Zero-based callable index within the batch.
    index: usize,
    /// Shared queue receiving successful callable outputs.
    outputs: Arc<SegQueue<(usize, R)>>,
}

impl<C, R> CallableTask<C, R> {
    /// Creates a runnable wrapper for one callable.
    ///
    /// # Parameters
    ///
    /// * `callable` - Callable to execute.
    /// * `index` - Zero-based callable index within the batch.
    /// * `outputs` - Shared queue receiving successful outputs.
    ///
    /// # Returns
    ///
    /// A runnable wrapper that sends successful output with its `index`.
    pub(crate) fn new(callable: C, index: usize, outputs: Arc<SegQueue<(usize, R)>>) -> Self {
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
    /// Executes the wrapped callable and enqueues its success value.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the callable succeeds, or the callable error when it
    /// fails.
    ///
    /// # Panics
    ///
    /// Panics if this wrapper is run more than once.
    fn run(&mut self) -> Result<(), E> {
        let mut callable = self
            .callable
            .take()
            .expect("callable task may only run once");
        let value = callable.call()?;
        self.outputs.push((self.index, value));
        Ok(())
    }
}
