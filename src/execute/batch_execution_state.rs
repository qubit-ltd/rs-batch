// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Mutex,
        MutexGuard,
    },
    time::Duration,
};

use qubit_atomic::AtomicCount;
use qubit_function::Runnable;
use qubit_progress::{
    MetricError,
    MetricHandle,
};

use crate::{
    BatchExecutionStateError,
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
    BatchTermination,
    execute::panic_payload_to_error,
};

/// Metric id used for task progress counters.
pub const EXECUTION_PROGRESS_METRIC_ID: &str = "tasks";

/// Metric display name used for task progress counters.
pub const EXECUTION_PROGRESS_METRIC_NAME: &str = "Tasks";

/// Shared state collected while a batch executor is running.
pub struct BatchExecutionState<E> {
    /// Declared task count.
    task_count: usize,
    /// Number of tasks observed from the source.
    observed_count: AtomicCount,
    /// Progress-owned lifecycle state for task counts.
    metric: MetricHandle,
    /// Detailed failures collected during execution.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> BatchExecutionState<E> {
    /// Creates empty execution state for a declared task count.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared number of tasks in the batch.
    ///
    /// # Returns
    ///
    /// Empty execution state.
    #[inline]
    pub fn new(task_count: usize, metric: MetricHandle) -> Self {
        Self {
            task_count,
            observed_count: AtomicCount::zero(),
            metric,
            failures: Mutex::new(Vec::new()),
        }
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
    /// # Returns
    ///
    /// `Ok(())` after the task's terminal outcome was recorded, or
    /// [`BatchExecutionStateError::TaskIndexOutOfRange`] before executing an
    /// out-of-range task.
    #[allow(deprecated)]
    pub fn execute_task<T>(
        &self,
        index: usize,
        mut task: T,
    ) -> Result<(), BatchExecutionStateError>
    where
        T: Runnable<E>,
    {
        if index >= self.task_count {
            return Err(BatchExecutionStateError::TaskIndexOutOfRange {
                index,
                task_count: self.task_count,
            });
        }
        self.record_task_started()?;
        match catch_unwind(AssertUnwindSafe(|| task.run())) {
            Ok(Ok(())) => self.record_task_succeeded()?,
            Ok(Err(error)) => self.record_task_failed(index, error)?,
            Err(payload) => self.record_task_panicked(
                index,
                panic_payload_to_error(payload.as_ref()),
            )?,
        }
        Ok(())
    }

    /// Records one observed task.
    ///
    /// # Returns
    ///
    /// The observed task count after this task was recorded.
    #[inline]
    pub fn record_task_observed(&self) -> usize {
        self.observed_count.inc()
    }

    /// Records that one task has started.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_started(&self) -> Result<(), MetricError> {
        self.metric.start(1)
    }

    /// Records one successful task completion.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_succeeded(&self) -> Result<(), MetricError> {
        self.metric.succeed(1)
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_failed(
        &self,
        index: usize,
        error: E,
    ) -> Result<(), MetricError> {
        self.metric.fail(1)?;
        Self::lock_failures(&self.failures)
            .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
        Ok(())
    }

    /// Records one task panic.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Captured task panic.
    ///
    /// # Panics
    ///
    /// Panics if no active task was recorded for this completion.
    #[deprecated(
        since = "0.10.0",
        note = "use execute_task to keep execution state consistent"
    )]
    #[inline]
    pub fn record_task_panicked(
        &self,
        index: usize,
        error: BatchTaskError<E>,
    ) -> Result<(), MetricError> {
        self.metric.fail(1)?;
        Self::lock_failures(&self.failures)
            .push(BatchTaskFailure::new(index, error));
        Ok(())
    }

    /// Returns the number of task errors and captured task panics.
    ///
    /// # Returns
    ///
    /// The total terminal task failure count recorded so far.
    #[inline]
    pub fn failure_count(&self) -> usize {
        Self::lock_failures(&self.failures).len()
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
    #[inline]
    pub fn into_outcome(self, elapsed: Duration) -> BatchOutcome<E> {
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
    pub fn into_outcome_with_termination(
        self,
        elapsed: Duration,
        termination: BatchTermination,
    ) -> BatchOutcome<E> {
        self.try_into_outcome_with_termination(elapsed, termination)
            .expect("batch execution state should collect consistent counters")
    }

    /// Consumes this state and validates the resulting batch outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// A validated final or partial outcome, or a build error if low-level
    /// recording calls created inconsistent counters.
    #[inline]
    pub fn try_into_outcome(
        self,
        elapsed: Duration,
    ) -> Result<BatchOutcome<E>, crate::BatchOutcomeBuildError> {
        self.try_into_outcome_with_termination(
            elapsed,
            BatchTermination::Finished,
        )
    }

    /// Consumes this state, applies `termination`, and validates the outcome.
    ///
    /// # Parameters
    ///
    /// * `elapsed` - Monotonic elapsed duration.
    /// * `termination` - How the executor stopped consuming its task source.
    ///
    /// # Returns
    ///
    /// A validated final or partial outcome, or a build error if low-level
    /// recording calls created inconsistent counters.
    #[inline]
    pub fn try_into_outcome_with_termination(
        self,
        elapsed: Duration,
        termination: BatchTermination,
    ) -> Result<BatchOutcome<E>, crate::BatchOutcomeBuildError> {
        let snapshot = self
            .metric
            .snapshot()
            .expect("batch progress metric state should remain readable");
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed_count = failures
            .iter()
            .filter(|failure| failure.error().is_failed())
            .count();
        let panicked_count = failures.len() - failed_count;
        BatchOutcomeBuilder::builder(self.task_count)
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
    fn lock_failures(
        failures: &Mutex<Vec<BatchTaskFailure<E>>>,
    ) -> MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
        failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
