// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_function::Runnable;
use qubit_progress::AutoReporterStatus;
use qubit_progress::ProgressNotifier;

use super::BatchExecutionState;
use super::ParallelBatchTask;
use crate::sync::AtomicU64;
use crate::sync::Ordering;

/// Allocates identities for active execution contexts.
static NEXT_EXECUTION_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates the next nonzero execution identity without wrapping.
#[inline]
fn next_execution_id() -> u64 {
    NEXT_EXECUTION_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(1))
        .expect("parallel batch execution context id space exhausted")
}

/// Worker-facing context for one parallel batch execution.
///
/// Runtime-specific executors receive this context from the coordinator and
/// use it to accept and execute one-shot task tokens.
pub struct ParallelBatchExecutionContext<E> {
    /// Globally unique identity for this execution context.
    execution_id: u64,
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
            execution_id: next_execution_id(),
            state,
            notifier,
            status,
        }
    }

    /// Accepts one source task and assigns it a unique in-range token.
    ///
    /// Tasks are rejected after automatic progress reporting fails or after the
    /// declared task count has been exceeded. A count-exceeding observation is
    /// still recorded so the coordinator can return the precise count error.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task yielded by the scheduler's source.
    ///
    /// # Returns
    ///
    /// `Some(token)` when the task is accepted, or `None` when execution must
    /// stop accepting work.
    #[inline]
    pub fn accept_task<T>(&self, task: T) -> Option<ParallelBatchTask<T>> {
        if self.status.is_failed() {
            return None;
        }
        let observed_count = self.state.try_record_task_observed()?;
        if observed_count > self.state.task_count() {
            return None;
        }
        self.state.record_task_accepted();
        Some(ParallelBatchTask::new(self.execution_id, observed_count - 1, task))
    }

    /// Runs one accepted token and records its terminal task outcome.
    ///
    /// Task-returned errors and task panics are stored in the batch outcome;
    /// they are not returned as this method's error.
    ///
    /// # Parameters
    ///
    /// * `task` - Token accepted by [`Self::accept_task`].
    ///
    /// # Panics
    ///
    /// Panics if the token does not represent a valid accepted task or if the
    /// task accounting state violates its internal transition invariants.
    pub fn execute_task<T>(&self, task: ParallelBatchTask<T>)
    where
        T: Runnable<E>,
    {
        let (execution_id, index, task) = task.into_parts();
        assert_eq!(
            execution_id, self.execution_id,
            "parallel batch task belongs to a different execution context"
        );
        self.state
            .execute_task(index, task)
            .expect("accepted parallel batch task must have valid progress transitions");
        self.notifier.notify();
    }
}
