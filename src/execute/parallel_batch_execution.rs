// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{sync::Arc, thread, time::Duration};

use qubit_progress::{Metric, Progress, Reporter};

use super::{
    BatchExecutionError, BatchExecutionState, BatchOutcome, BatchOutcomeBuilder,
    ParallelBatchExecutionContext, EXECUTION_PROGRESS_METRIC_ID,
    EXECUTION_PROGRESS_METRIC_NAME,
};
use crate::ProgressFailure;

/// Shared lifecycle driver for runtime-specific parallel batch executors.
///
/// The driver owns progress creation and termination, source-count validation,
/// task-outcome finalization, and conversion of reporter failures into
/// [`BatchExecutionError`]. A runtime adapter supplies only scheduling: it
/// accepts tasks, calls methods on [`ParallelBatchExecutionContext`], joins all
/// workers, and returns the observed source count.
pub enum ParallelBatchExecution {}

impl ParallelBatchExecution {
    /// Runs a batch through a runtime-specific parallel scheduler.
    ///
    /// The scheduler is invoked while an automatic progress reporter is active.
    /// It must join every worker before returning, stop accepting work when
    /// [`ParallelBatchExecutionContext::reporting_failed`] becomes `true`, and
    /// return the count obtained from
    /// [`ParallelBatchExecutionContext::record_task_observed`].
    ///
    /// # Type Parameters
    ///
    /// * `I` - Source of scheduler-owned task values.
    /// * `E` - Task-specific error type stored in the batch outcome.
    /// * `S` - Runtime-specific scheduler closure.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source consumed exactly once by `schedule`.
    /// * `count` - Declared task count expected from `tasks`.
    /// * `reporter` - Progress reporter receiving lifecycle callbacks.
    /// * `report_interval` - Minimum interval between running progress updates.
    /// * `schedule` - Scheduler that dispatches tasks and returns observed count.
    ///
    /// # Returns
    ///
    /// A complete batch outcome when source count and reporting succeed, or a
    /// batch error containing the partial outcome.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress setup,
    /// automatic reporting, or terminal reporting fails;
    /// [`BatchExecutionError::CountShortfall`] when the fully consumed source
    /// ends early; or [`BatchExecutionError::CountExceeded`] after observing an
    /// extra source task.
    ///
    /// # Panics
    ///
    /// Propagates scheduler panics, including worker panics that the scheduler
    /// joins and resumes. Task panics are captured when the scheduler calls
    /// [`ParallelBatchExecutionContext::execute_task`].
    pub fn run<I, E, S>(
        tasks: I,
        count: usize,
        reporter: Arc<dyn Reporter>,
        report_interval: Duration,
        schedule: S,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator,
        E: Send,
        S: FnOnce(I, usize, ParallelBatchExecutionContext<E>) -> usize,
    {
        let mut progress = match Progress::builder_arc(reporter)
            .interval(report_interval)
            .metric(
                Metric::new(EXECUTION_PROGRESS_METRIC_ID, EXECUTION_PROGRESS_METRIC_NAME)
                    .total(count as u64),
            )
            .start()
        {
            Ok(progress) => progress,
            Err(source) => {
                return Err(BatchExecutionError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    outcome: Self::empty_outcome(count),
                });
            }
        };
        let metric = progress
            .metric(EXECUTION_PROGRESS_METRIC_ID)
            .expect("configured execution metric must exist");
        let state = Arc::new(BatchExecutionState::new(count, metric));

        let running_result = thread::scope(|scope| {
            let running_progress = progress.spawn_auto_reporter(scope);
            let context = ParallelBatchExecutionContext::new(
                Arc::clone(&state),
                running_progress.notifier(),
                running_progress.status(),
            );
            let observed_count = schedule(tasks, count, context);
            running_progress.stop().map(|()| observed_count)
        });

        let state = Arc::into_inner(state)
            .expect("parallel batch execution state should have a single owner");
        let observed_count = match running_result {
            Ok(observed_count) => observed_count,
            Err(source) => {
                return Err(BatchExecutionError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    outcome: state.into_outcome(progress.elapsed()),
                });
            }
        };
        Self::finish(progress, state, count, observed_count)
    }

    /// Builds an empty outcome for a progress setup failure.
    ///
    /// # Parameters
    ///
    /// * `count` - Declared task count for the failed batch setup.
    ///
    /// # Returns
    ///
    /// A valid outcome with no completed tasks.
    #[inline]
    fn empty_outcome<E>(count: usize) -> BatchOutcome<E> {
        BatchOutcomeBuilder::builder(count)
            .elapsed(Duration::ZERO)
            .build()
            .expect("empty batch outcome must be valid")
    }

    /// Emits the terminal progress event and maps final scheduler state.
    ///
    /// # Parameters
    ///
    /// * `progress` - Active progress run for the batch.
    /// * `state` - Completed task accounting state.
    /// * `count` - Declared task count.
    /// * `observed_count` - Count returned by the scheduler.
    ///
    /// # Returns
    ///
    /// The final outcome or a batch-level progress/count error.
    fn finish<E>(
        progress: Progress<'_>,
        state: BatchExecutionState<E>,
        count: usize,
        observed_count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>> {
        if observed_count < count {
            let (elapsed, report_error) = Self::fail_progress(progress);
            return Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: observed_count,
                outcome: state.into_outcome(elapsed),
                report_error,
            });
        }
        if observed_count > count {
            let (elapsed, report_error) = Self::fail_progress(progress);
            return Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: observed_count,
                outcome: state.into_outcome(elapsed),
                report_error,
            });
        }

        let terminal = if state.failure_count() > 0 {
            progress.fail().map_err(|source| {
                (source.elapsed(), ProgressFailure::from(source))
            })
        } else {
            progress.finish().map_err(|source| {
                (
                    source.elapsed(),
                    ProgressFailure::from_finish_error(source),
                )
            })
        };
        match terminal {
            Ok(elapsed) => Ok(state.into_outcome(elapsed)),
            Err((elapsed, source)) => Err(BatchExecutionError::ProgressReport {
                source: Box::new(source),
                outcome: state.into_outcome(elapsed),
            }),
        }
    }

    /// Reports a failed terminal phase while preserving a secondary error.
    ///
    /// # Parameters
    ///
    /// * `progress` - Active progress run to mark failed.
    ///
    /// # Returns
    ///
    /// Terminal elapsed time and an optional progress-report failure.
    fn fail_progress(
        progress: Progress<'_>,
    ) -> (Duration, Option<Box<ProgressFailure>>) {
        match progress.fail() {
            Ok(elapsed) => (elapsed, None),
            Err(source) => (
                source.elapsed(),
                Some(Box::new(ProgressFailure::from(source))),
            ),
        }
    }
}
