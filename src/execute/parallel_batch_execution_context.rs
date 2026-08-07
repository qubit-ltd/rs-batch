// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_function::Runnable;
use qubit_progress::{
    AutoReporterStatus,
    ProgressNotifier,
};

use super::{
    BatchExecutionState,
    ParallelBatchExecutionContextError,
};

/// Worker-facing context for one parallel batch execution.
///
/// Runtime-specific executors receive this context from the coordinator and use
/// it to observe source tasks, execute accepted tasks, and detect
/// auto-reporting terminal failures.
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
    /// This constructor is only intended for runtime executors.
    /// Most callers should use
    /// [`crate::execute::ParallelBatchExecutionCoordinator::execute`].
    #[inline]
    pub(crate) fn new(
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
    /// Schedulers should stop accepting and executing new work when this is
    /// `true`.
    #[inline]
    pub fn reporting_failed(&self) -> bool {
        self.status.is_failed()
    }

    /// Runs one accepted task and records its terminal task outcome.
    ///
    /// Task-returned errors and task panics are stored in the batch outcome;
    /// they are not returned as this method's error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index within the declared batch.
    /// * `task` - Accepted task to execute exactly once.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the accepted task reaches one of the terminal states and
    /// running progress was notified.
    pub fn execute_task<T>(
        &self,
        index: usize,
        task: T,
    ) -> Result<(), ParallelBatchExecutionContextError>
    where
        T: Runnable<E>,
    {
        self.state.execute_task(index, task)?;
        self.notifier.notify();
        Ok(())
    }
}
