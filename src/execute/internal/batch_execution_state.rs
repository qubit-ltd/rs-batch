// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;

use qubit_function::Runnable;
use qubit_progress::MetricError;
use qubit_progress::MetricHandle;

use super::ParallelBatchAcceptanceState;
use super::TaskExecutionStatus;
use crate::BatchOutcome;
use crate::BatchOutcomeBuilder;
use crate::BatchTaskError;
use crate::BatchTaskFailure;
use crate::BatchTermination;
use crate::TaskFailurePolicy;
use crate::execute::panic_payload_to_error;
use crate::sync::AtomicBool;
use crate::sync::AtomicUsize;
use crate::sync::Ordering;

/// Metric id used for task progress counters.
pub(crate) const EXECUTION_PROGRESS_METRIC_ID: &str = "tasks";

/// Metric display name used for task progress counters.
pub(crate) const EXECUTION_PROGRESS_METRIC_NAME: &str = "Tasks";

/// Shared state collected while a batch executor is running.
pub(crate) struct BatchExecutionState<E> {
    /// Atomic source-admission counters and stop state.
    acceptance: ParallelBatchAcceptanceState,
    /// Progress-owned lifecycle state for task counts.
    metric: MetricHandle,
    /// Detailed failures collected during execution.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
    /// Policy controlling whether new tasks are accepted after failures.
    task_failure_policy: TaskFailurePolicy,
    /// Number of task failures observed by parallel workers.
    failure_count_atomic: AtomicUsize,
    /// Whether the task source has been observed to be exhausted.
    source_exhausted: AtomicBool,
}

impl<E> BatchExecutionState<E> {
    /// Creates empty execution state for a declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared task count for the batch.
    /// * `metric` - Progress metric whose transitions track task lifecycle.
    /// * `task_failure_policy` - Cooperative admission stop policy.
    ///
    /// # Returns
    ///
    /// Empty execution state.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) fn new(
        task_count: usize,
        metric: MetricHandle,
        task_failure_policy: TaskFailurePolicy,
    ) -> Self {
        Self {
            acceptance: ParallelBatchAcceptanceState::new(task_count),
            metric,
            failures: Mutex::new(Vec::new()),
            task_failure_policy,
            failure_count_atomic: AtomicUsize::new(0),
            source_exhausted: AtomicBool::new(false),
        }
    }

    /// Returns the declared task count used by the active execution.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) const fn task_count(&self) -> usize {
        self.acceptance.task_count()
    }

    /// Returns the number of source tasks observed by the scheduler.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn observed_count(&self) -> usize {
        self.acceptance.observed_count()
    }

    /// Returns the number of source tasks accepted for execution.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn accepted_count(&self) -> usize {
        self.acceptance.accepted_count()
    }

    /// Returns the number of tasks that reached a terminal metric state.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn completed_count(&self) -> usize {
        self.metric.snapshot().completed() as usize
    }

    /// Returns the number of task errors and captured task panics.
    ///
    /// # Returns
    ///
    /// The total terminal task failure count recorded so far.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn failure_count(&self) -> usize {
        Self::lock_failures(&self.failures).len()
    }

    /// Returns whether the failure policy has stopped accepting new tasks.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn should_stop_accepting(&self) -> bool {
        self.acceptance.should_stop()
    }

    /// Returns whether the source iterator has been observed to be exhausted.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub(crate) fn source_exhausted(&self) -> bool {
        self.source_exhausted.load(Ordering::Acquire)
    }

    /// Executes one indexed task and records its terminal outcome.
    ///
    /// Task-returned errors and captured panics are stored in this state and do
    /// not become this method's error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based index of `task` within the declared batch.
    /// * `task` - Runnable task executed synchronously by this call.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    ///
    /// # Returns
    ///
    /// The terminal status for this task.
    ///
    /// # Errors
    ///
    /// Returns a metric error when a progress lifecycle transition is rejected.
    #[inline(always)]
    pub(crate) fn execute_task<T>(
        &self,
        index: usize,
        mut task: T,
    ) -> Result<TaskExecutionStatus, MetricError>
    where
        T: Runnable<E>,
    {
        self.execute_action(index, || task.run())
    }

    /// Executes one indexed action and records its terminal outcome.
    ///
    /// Action-returned errors and captured panics are stored in this state and
    /// do not become this method's error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based index of the action within the declared batch.
    /// * `action` - Fallible action executed synchronously by this call.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Fallible action type.
    ///
    /// # Returns
    ///
    /// The terminal status for this action.
    ///
    /// # Errors
    ///
    /// Returns a metric error when a progress lifecycle transition is rejected.
    pub(crate) fn execute_action<F>(
        &self,
        index: usize,
        action: F,
    ) -> Result<TaskExecutionStatus, MetricError>
    where
        F: FnOnce() -> Result<(), E>,
    {
        self.metric.start(1)?;
        let status = match catch_unwind(AssertUnwindSafe(action)) {
            Ok(Ok(())) => {
                self.metric.succeed(1)?;
                TaskExecutionStatus::Succeeded
            }
            Ok(Err(error)) => {
                self.metric.fail(1)?;
                Self::lock_failures(&self.failures)
                    .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
                TaskExecutionStatus::Failed
            }
            Err(payload) => {
                self.metric.fail(1)?;
                Self::lock_failures(&self.failures).push(BatchTaskFailure::new(
                    index,
                    panic_payload_to_error(payload.as_ref()),
                ));
                TaskExecutionStatus::Failed
            }
        };
        if status == TaskExecutionStatus::Failed {
            let failures = self.failure_count_atomic.fetch_add(1, Ordering::AcqRel) + 1;
            if self.task_failure_policy.should_stop(failures) {
                self.acceptance.stop();
            }
        }
        Ok(status)
    }

    /// Records one observed task.
    ///
    /// # Returns
    ///
    /// The observed task count after this task was recorded.
    #[inline(always)]
    pub(crate) fn record_task_observed(&self) -> usize {
        self.acceptance.record_observed()
    }

    /// Records one observed task unless failure policy already stopped
    /// admission.
    ///
    /// # Returns
    ///
    /// `Some(count)` with the new observed count when admission is still open,
    /// or `None` when the failure policy has already stopped accepting tasks.
    #[inline(always)]
    pub(crate) fn try_record_task_observed(&self) -> Option<usize> {
        self.acceptance.try_record_observed()
    }

    /// Records one source task accepted for execution.
    ///
    /// # Returns
    ///
    /// The accepted task count after this task was recorded.
    #[inline(always)]
    pub(crate) fn record_task_accepted(&self) -> usize {
        self.acceptance.record_accepted()
    }

    /// Marks the source iterator as exhausted.
    ///
    /// Subsequent [`Self::source_exhausted`] queries observe `true` even when
    /// the declared task count has not yet been reached.
    #[inline]
    pub(crate) fn mark_source_exhausted(&self) {
        self.source_exhausted.store(true, Ordering::Release);
    }

    /// Consumes this state and builds a batch outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The final or partial outcome represented by this state.
    ///
    /// # Panics
    ///
    /// Panics if callers used the low-level recording methods to create
    /// counters or failure details that violate [`BatchOutcome`] invariants.
    #[inline(always)]
    pub(crate) fn into_outcome(self, elapsed: Duration) -> BatchOutcome<E> {
        self.into_outcome_with_termination(elapsed, BatchTermination::Finished)
    }

    /// Consumes this state and builds a batch outcome with `termination`.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    /// * `termination` - How the executor stopped consuming its task source.
    ///
    /// # Returns
    ///
    /// The final or partial outcome represented by this state.
    ///
    /// # Panics
    ///
    /// Panics if callers used the low-level recording methods to create
    /// counters or failure details that violate [`BatchOutcome`] invariants.
    #[inline]
    pub(crate) fn into_outcome_with_termination(
        self,
        elapsed: Duration,
        termination: BatchTermination,
    ) -> BatchOutcome<E> {
        self.try_into_outcome_with_termination(elapsed, termination)
            .expect("batch execution state should collect consistent counters")
    }

    /// Consumes this state, applies `termination`, and validates the outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    /// * `termination` - How execution stopped consuming the task source.
    ///
    /// # Returns
    ///
    /// A validated final or partial outcome, or a build error if low-level
    /// recording calls created inconsistent counters.
    pub(crate) fn try_into_outcome_with_termination(
        self,
        elapsed: Duration,
        termination: BatchTermination,
    ) -> Result<BatchOutcome<E>, crate::BatchOutcomeBuildError> {
        let snapshot = self.metric.snapshot();
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed_count = failures
            .iter()
            .filter(|failure| failure.error().is_failed())
            .count();
        let panicked_count = failures.len() - failed_count;
        BatchOutcomeBuilder::builder(self.acceptance.task_count())
            .completed_count(snapshot.completed() as usize)
            .succeeded_count(snapshot.succeeded() as usize)
            .failed_count(failed_count)
            .panicked_count(panicked_count)
            .termination(termination)
            .elapsed(elapsed)
            .failures(failures)
            .build()
    }

    /// Acquires the failure list lock while tolerating poisoned locks.
    ///
    /// # Parameters
    ///
    /// * `failures` - Failure list mutex to lock.
    ///
    /// # Returns
    ///
    /// A guard for the failure list.
    #[inline(always)]
    fn lock_failures(
        failures: &Mutex<Vec<BatchTaskFailure<E>>>,
    ) -> MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
        failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
