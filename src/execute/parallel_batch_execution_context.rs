// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_function::Runnable;
use qubit_progress::{AutoReporterStatus, ProgressNotifier};

use super::{BatchExecutionState, BatchExecutionStateError};

/// Worker-facing state for one parallel batch execution.
///
/// Runtime-specific executors receive this context from
/// [`ParallelBatchExecution`](super::ParallelBatchExecution). They use it to
/// observe source tasks, execute accepted tasks, and cooperate with automatic
/// progress-reporting failure. The context is cloneable so scoped worker tasks
/// can share the same accounting state without exposing progress internals.
///
/// # Type Parameters
///
/// * `E` - Task-specific error type recorded for returned task errors.
#[derive(Clone)]
pub struct ParallelBatchExecutionContext<E> {
    /// Shared task accounting and failure collection state.
    state: Arc<BatchExecutionState<E>>,
    /// Handle used to wake the automatic running-progress reporter.
    notifier: ProgressNotifier,
    /// Shared status used to stop accepting work after reporter failure.
    status: AutoReporterStatus,
}

impl<E> ParallelBatchExecutionContext<E> {
    /// Creates worker-facing execution state for one active batch run.
    ///
    /// This constructor is only intended for runtime adapters. Most callers
    /// should use [`ParallelBatchExecution::run`](super::ParallelBatchExecution::run).
    ///
    /// # Parameters
    ///
    /// * `state` - Shared batch accounting state.
    /// * `notifier` - Automatic-reporter wakeup handle.
    /// * `status` - Automatic-reporter terminal status.
    ///
    /// # Returns
    ///
    /// A context that records work for the active batch run.
    #[inline]
    pub fn new(
        state: Arc<BatchExecutionState<E>>,
        notifier: ProgressNotifier,
        status: AutoReporterStatus,
    ) -> Self {
        Self {
            state,
            notifier,
            status,
        }
    }

    /// Records one task observed from the source.
    ///
    /// # Returns
    ///
    /// The total number of observed tasks after this observation.
    #[inline]
    pub fn record_task_observed(&self) -> usize {
        self.state.record_task_observed()
    }

    /// Returns whether automatic progress reporting has failed.
    ///
    /// Schedulers should stop accepting and executing new work when this
    /// returns `true`; [`ParallelBatchExecution::run`](super::ParallelBatchExecution::run)
    /// will return the captured progress failure with the partial outcome.
    ///
    /// # Returns
    ///
    /// `true` when automatic progress reporting has reached a terminal error.
    #[inline]
    pub fn reporting_failed(&self) -> bool {
        self.status.is_failed()
    }

    /// Runs one accepted task and records its terminal task outcome.
    ///
    /// Task-returned errors and task panics are stored in the batch outcome;
    /// they do not become this method's error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index within the declared batch.
    /// * `task` - Accepted task to execute exactly once.
    ///
    /// # Returns
    ///
    /// `Ok(())` after recording a terminal task outcome, or
    /// [`BatchExecutionStateError::TaskIndexOutOfRange`] before running an
    /// invalid index.
    pub fn execute_task<T>(
        &self,
        index: usize,
        task: T,
    ) -> Result<(), BatchExecutionStateError>
    where
        T: Runnable<E>,
    {
        self.state.execute_task(index, task)
    }

    /// Signals that a worker reached a task terminal point.
    ///
    /// The signal prompts zero-interval automatic reporters to emit a running
    /// update and wakes interval-based reporters without forcing an emission.
    #[inline]
    pub fn notify_task_terminal(&self) {
        self.notifier.notify();
    }
}
