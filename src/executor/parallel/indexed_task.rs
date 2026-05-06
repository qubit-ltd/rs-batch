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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, mpsc};

use qubit_function::Runnable;

use crate::error::panic_payload_to_error;

use super::{
    parallel_batch_progress_state::ParallelBatchProgressState,
    parallel_batch_result_state::ParallelBatchResultState,
};

/// Indexed task submitted to scoped workers.
pub(crate) struct IndexedTask<T> {
    /// Zero-based task index within the batch.
    pub(crate) index: usize,
    /// Task payload.
    pub(crate) task: T,
}

/// Runs tasks from a shared receiver until the channel closes.
///
/// # Parameters
///
/// * `task_receiver` - Shared receiver protected because standard receivers are
///   not `Sync`.
/// * `progress_state` - Shared progress counters.
/// * `result_state` - Shared final result state.
pub(crate) fn run_parallel_worker<T, E>(
    task_receiver: Arc<Mutex<mpsc::Receiver<IndexedTask<T>>>>,
    progress_state: Arc<ParallelBatchProgressState>,
    result_state: Arc<ParallelBatchResultState<E>>,
) where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    loop {
        let received = task_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        let Ok(indexed_task) = received else {
            break;
        };
        run_parallel_task(&progress_state, &result_state, indexed_task);
    }
}

fn run_parallel_task<T, E>(
    progress_state: &ParallelBatchProgressState,
    result_state: &ParallelBatchResultState<E>,
    indexed_task: IndexedTask<T>,
) where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    let IndexedTask { index, mut task } = indexed_task;
    progress_state.record_task_started();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    match outcome {
        Ok(Ok(())) => {
            progress_state.record_task_succeeded();
            result_state.record_task_succeeded();
        }
        Ok(Err(error)) => {
            progress_state.record_task_failed();
            result_state.record_task_failed(index, error);
        }
        Err(payload) => {
            progress_state.record_task_panicked();
            result_state.record_task_panicked(index, panic_payload_to_error(payload.as_ref()));
        }
    }
}
