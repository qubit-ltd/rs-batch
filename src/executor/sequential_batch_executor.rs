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

use crate::{
    BatchExecutionError,
    BatchExecutionState,
    BatchOutcome,
    NoOpProgressReporter,
    ProgressPhase,
    ProgressReporter,
    ProgressRun,
    error::panic_payload_to_error,
};

use super::BatchExecutor;

/// Executes a whole batch sequentially on the caller thread.
///
/// Progress updates are emitted only between tasks. A long-running single task
/// therefore does not produce intermediate sequential progress callbacks.
///
#[derive(Clone)]
pub struct SequentialBatchExecutor {
    /// Interval between progress callbacks while the batch is running.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl SequentialBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Creates a sequential batch executor with default configuration.
    ///
    /// # Returns
    ///
    /// A sequential batch executor using [`NoOpProgressReporter`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later executions.
    ///
    /// # Returns
    ///
    /// A new executor that shares the supplied reporter.
    #[inline]
    pub fn with_reporter<R>(self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.with_reporter_arc(Arc::new(reporter))
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later executions.
    ///
    /// # Returns
    ///
    /// A new executor that shares the supplied reporter.
    #[inline]
    pub fn with_reporter_arc(self, reporter: Arc<dyn ProgressReporter>) -> Self {
        Self { reporter, ..self }
    }

    /// Returns a copy configured with the supplied progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between progress callbacks.
    ///
    /// # Returns
    ///
    /// A new executor using `report_interval`.
    #[inline]
    pub fn with_report_interval(self, report_interval: Duration) -> Self {
        Self {
            report_interval,
            ..self
        }
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between progress callbacks.
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
    /// A sequential batch executor using [`NoOpProgressReporter`].
    fn default() -> Self {
        Self {
            report_interval: Self::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
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
    fn execute<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send + std::fmt::Debug,
    {
        let mut state = BatchExecutionState::new(count);
        let mut progress = ProgressRun::new(self.reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            state.progress_counters(),
            Duration::ZERO,
        );
        let mut actual_count = 0;
        for task in tasks {
            if actual_count == count {
                let elapsed = progress.elapsed();
                progress.report_with_elapsed(
                    ProgressPhase::Failed,
                    state.progress_counters(),
                    elapsed,
                );
                let outcome = state.into_outcome(elapsed);
                return Err(BatchExecutionError::CountExceeded {
                    expected: count,
                    observed_at_least: count + 1,
                    outcome,
                });
            }
            execute_one_task(task, actual_count, &mut state);
            actual_count += 1;
            progress.report_running_if_due(state.progress_counters());
        }

        let elapsed = progress.elapsed();
        if actual_count < count {
            progress.report_with_elapsed(ProgressPhase::Failed, state.progress_counters(), elapsed);
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: state.into_outcome(elapsed),
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                state.progress_counters(),
                elapsed,
            );
            Ok(state.into_outcome(elapsed))
        }
    }
}

/// Executes one task and updates sequential counters.
///
/// # Parameters
///
/// * `task` - Task to execute.
/// * `index` - Zero-based task index within the batch.
/// * `completed_count` - Completed-task counter updated on return.
/// * `succeeded_count` - Successful-task counter updated on return.
/// * `failed_count` - Failed-task counter updated on return.
/// * `panicked_count` - Panicked-task counter updated on return.
/// * `failures` - Failure list appended when the task fails or panics.
fn execute_one_task<T, E>(mut task: T, index: usize, state: &mut BatchExecutionState<E>)
where
    T: Runnable<E>,
    E: std::fmt::Debug,
{
    state.record_task_started();
    match catch_unwind(AssertUnwindSafe(|| task.run())) {
        Ok(Ok(())) => state.record_task_succeeded(),
        Ok(Err(error)) => state.record_task_failed(index, error),
        Err(payload) => state.record_task_panicked(index, panic_payload_to_error(payload.as_ref())),
    }
}
