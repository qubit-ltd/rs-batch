// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    sync::Arc,
    thread,
    time::Duration,
};

use qubit_progress::{
    Metric,
    Progress,
    Reporter,
};

use super::{
    BatchExecutionError,
    BatchExecutionState,
    BatchOutcome,
    BatchOutcomeBuilder,
    EXECUTION_PROGRESS_METRIC_ID,
    EXECUTION_PROGRESS_METRIC_NAME,
    ParallelBatchExecutionContext,
};
use crate::ProgressFailure;

/// Shared coordinator for runtime-specific parallel execution paths.
///
/// The coordinator owns progress setup and finalization, state construction,
/// scheduler execution, and count/termination validation.
#[derive(Clone)]
pub struct ParallelBatchExecutionCoordinator {
    /// Progress reporter used by this execution.
    reporter: Arc<dyn Reporter>,
    /// Minimum interval between due-based progress updates.
    report_interval: Duration,
}

impl ParallelBatchExecutionCoordinator {
    /// Creates a coordinator instance.
    #[inline]
    pub fn new(reporter: Arc<dyn Reporter>, report_interval: Duration) -> Self {
        Self {
            reporter,
            report_interval,
        }
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between due-based running progress events.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the reporter used for the current coordinator.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub const fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.reporter
    }

    /// Executes one batch through a scheduler closure.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source consumed by the scheduler.
    /// * `count` - Declared task count expected from `tasks`.
    /// * `schedule` - Runtime-specific scheduler that consumes tasks.
    pub fn execute<I, E, S>(
        &self,
        tasks: I,
        count: usize,
        schedule: S,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator,
        E: Send,
        S: FnOnce(I, usize, &ParallelBatchExecutionContext<E>) -> usize,
    {
        let mut progress =
            match Progress::builder_arc(Arc::clone(&self.reporter))
                .interval(self.report_interval)
                .metric(
                    Metric::new(
                        EXECUTION_PROGRESS_METRIC_ID,
                        EXECUTION_PROGRESS_METRIC_NAME,
                    )
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

        let stop_result = thread::scope(|scope| {
            let running_progress = progress.spawn_auto_reporter(scope);
            let context = ParallelBatchExecutionContext::new(
                Arc::clone(&state),
                running_progress.notifier(),
                running_progress.status(),
            );
            let observed_count = schedule(tasks, count, &context);
            running_progress.stop().map(|()| observed_count)
        });
        let state = Arc::into_inner(state).expect(
            "parallel batch execution state should have a single owner",
        );
        let observed_count = match stop_result {
            Ok(count) => count,
            Err(source) => {
                return Err(BatchExecutionError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    outcome: state.into_outcome(progress.elapsed()),
                });
            }
        };

        Self::finish(progress, state, count, observed_count)
    }

    /// Returns a zero-completion outcome for immediate setup failures.
    #[inline]
    fn empty_outcome<E>(count: usize) -> BatchOutcome<E> {
        BatchOutcomeBuilder::builder(count)
            .elapsed(Duration::ZERO)
            .build()
            .expect("empty batch outcome must be valid")
    }

    /// Finalizes completion, reports terminal progress, and maps count errors.
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
                (source.elapsed(), ProgressFailure::from_finish_error(source))
            })
        };

        match terminal {
            Ok(elapsed) => Ok(state.into_outcome(elapsed)),
            Err((elapsed, source)) => {
                Err(BatchExecutionError::ProgressReport {
                    source: Box::new(source),
                    outcome: state.into_outcome(elapsed),
                })
            }
        }
    }

    /// Reports a failed terminal phase and returns elapsed with secondary
    /// error.
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
