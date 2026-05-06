/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::fmt;
use std::panic::{
    AssertUnwindSafe,
    catch_unwind,
};

use qubit_function::Runnable;

use crate::execute::{
    BatchExecutionState,
    panic_payload_to_error,
};

/// Runs one indexed task and updates shared execution state.
///
/// # Parameters
///
/// * `state` - Shared execution state.
/// * `index` - Zero-based task index.
/// * `task` - Runnable task to execute.
pub(crate) fn run_parallel_task<T, E>(state: &BatchExecutionState<E>, index: usize, mut task: T)
where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    state.record_task_started();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    match outcome {
        Ok(Ok(())) => {
            state.record_task_succeeded();
        }
        Ok(Err(error)) => {
            state.record_task_failed(index, error);
        }
        Err(payload) => {
            state.record_task_panicked(index, panic_payload_to_error(payload.as_ref()));
        }
    }
}
