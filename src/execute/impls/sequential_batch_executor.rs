/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::Arc,
    time::Duration,
};

use qubit_function::Runnable;
use qubit_progress::{
    Progress,
    reporter::ProgressReporter,
};

use crate::{
    BatchExecutionError,
    BatchOutcome,
    execute::{
        BatchExecutionState,
        BatchExecutor,
        panic_payload_to_error,
    },
};

use super::SequentialBatchExecutorBuilder;

/// Executes a whole batch sequentially on the caller thread.
///
/// Progress updates are emitted only between tasks. A long-running single task
/// therefore does not produce intermediate sequential progress callbacks.
///
/// ```rust
/// use qubit_batch::{
///     BatchExecutor,
///     SequentialBatchExecutor,
/// };
///
/// let outcome = SequentialBatchExecutor::new()
///     .for_each(["a", "b", "c"], |item| {
///         assert!(!item.is_empty());
///         Ok::<(), &'static str>(())
///     })
///     .expect("array length should be exact");
///
/// assert!(outcome.is_success());
/// ```
#[derive(Clone)]
pub struct SequentialBatchExecutor {
    /// Interval between progress callbacks while the batch is running.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn ProgressReporter>,
}

impl SequentialBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Creates a sequential batch executor with default configuration.
    ///
    /// # Returns
    ///
    /// A sequential batch executor using no-op progress reporting.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a builder for configuring a sequential batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[inline]
    pub fn builder() -> SequentialBatchExecutorBuilder {
        SequentialBatchExecutorBuilder::default()
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between due-based running progress callbacks.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.reporter
    }
}

impl Default for SequentialBatchExecutor {
    /// Creates a sequential batch executor with default configuration.
    ///
    /// # Returns
    ///
    /// A sequential batch executor using no-op progress reporting.
    #[inline]
    fn default() -> Self {
        Self::builder().build()
    }
}

impl BatchExecutor for SequentialBatchExecutor {
    /// Executes the batch sequentially on the caller thread.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source for the batch.
    /// * `count` - Declared task count expected from `tasks`.
    ///
    /// # Returns
    ///
    /// A structured batch result when the declared task count matches, or a
    /// batch-count mismatch error with the attached partial result.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when `tasks` yields fewer or more tasks
    /// than `count`.
    ///
    /// # Panics
    ///
    /// Panics from tasks are captured in the result. Panics from the configured
    /// progress reporter are propagated to the caller.
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        let state = BatchExecutionState::new(count);
        let mut progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_started(state.progress_counters());
        let mut actual_count = 0;
        for task in tasks {
            actual_count = state.record_task_observed();
            if actual_count > count {
                let failed = progress.report_failed(state.progress_counters());
                let outcome = state.into_outcome(failed.elapsed());
                return Err(BatchExecutionError::CountExceeded {
                    expected: count,
                    observed_at_least: actual_count,
                    outcome,
                });
            }
            // Execute the task and update the state.
            let mut task = task;
            state.record_task_started();
            match catch_unwind(AssertUnwindSafe(|| task.run())) {
                Ok(Ok(())) => state.record_task_succeeded(),
                Ok(Err(error)) => state.record_task_failed(actual_count - 1, error),
                Err(payload) => state.record_task_panicked(
                    actual_count - 1,
                    panic_payload_to_error(payload.as_ref()),
                ),
            }
            // Update the actual task count and report progress if due.
            let _ = progress.report_running_if_due(state.progress_counters());
        }

        if actual_count < count {
            let failed = progress.report_failed(state.progress_counters());
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: state.into_outcome(failed.elapsed()),
            })
        } else {
            let finished = progress.report_finished(state.progress_counters());
            Ok(state.into_outcome(finished.elapsed()))
        }
    }
}
