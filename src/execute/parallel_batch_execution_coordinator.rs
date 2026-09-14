// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_progress::Metric;
use qubit_progress::Progress;
use qubit_progress::Reporter;

use super::BatchExecutionError;
use super::BatchExecutionState;
use super::BatchOutcome;
use super::BatchOutcomeBuilder;
use super::EXECUTION_PROGRESS_METRIC_ID;
use super::EXECUTION_PROGRESS_METRIC_NAME;
use super::ParallelBatchExecutionContext;
use super::TaskFailurePolicy;
use super::parallel_batch_source::ParallelBatchSource;
use crate::ProgressFailure;

/// Shared coordinator for runtime-specific parallel execution paths.
///
/// The coordinator owns progress setup and finalization, state construction,
/// scheduler execution, and count/termination validation.
///
/// # Examples
///
/// ```rust
/// use std::convert::Infallible;
/// use std::sync::Arc;
/// use std::time::Duration;
/// use qubit_batch::TaskFailurePolicy;
/// use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
/// use qubit_progress::NoopReporter;
/// let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
/// let outcome = coordinator.execute_with_source([|| Ok::<(), &'static str>(())], 1,
///     TaskFailurePolicy::Continue, |source, context| {
///         for token in source {
///             context.execute_task(token);
///         }
///         Ok::<(), Infallible>(())
///     }).expect("all accepted tasks finish and the source is exhausted");
/// assert!(outcome.is_success());
/// ```
#[derive(Clone)]
pub struct ParallelBatchExecutionCoordinator {
    /// Progress reporter used by this execution.
    reporter: Arc<dyn Reporter>,
    /// Minimum interval between due-based progress updates.
    report_interval: Duration,
}

impl ParallelBatchExecutionCoordinator {
    /// Creates a coordinator instance.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Reporter receiving lifecycle and running progress events.
    /// * `report_interval` - Minimum interval between due-based running events.
    ///
    /// # Returns
    ///
    /// A coordinator configured with the supplied reporter and interval.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub fn new(reporter: Arc<dyn Reporter>, report_interval: Duration) -> Self {
        Self {
            reporter,
            report_interval,
        }
    }

    /// Executes a batch while owning the source admission boundary.
    ///
    /// This is the only public scheduling entry point. It prevents schedulers
    /// from substituting a different source or bypassing exhaustion validation.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Task source type.
    /// * `E` - Task-specific error type.
    /// * `S` - Scheduler error type.
    /// * `Schedule` - Runtime-specific scheduling closure type.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Source converted lazily when the scheduler first pulls it.
    /// * `count` - Declared number of tasks expected from the source.
    /// * `task_failure_policy` - Policy that may stop source admission after
    ///   task failures while accepted tasks finish.
    /// * `schedule` - Scheduler that consumes the supplied
    ///   [`ParallelBatchSource`] and executes every accepted token before
    ///   returning. Returning early without a policy stop is an incomplete
    ///   schedule, even after `count` tasks have completed.
    ///
    /// # Returns
    ///
    /// A validated [`BatchOutcome`] when the source is exhausted or a failure
    /// policy stops admission and all accepted tasks have completed.
    ///
    /// # Errors
    ///
    /// Returns a schedule, progress-report, or source-count error when the
    /// corresponding operation fails. Returns
    /// [`BatchExecutionError::IncompleteSchedule`] when accepted tasks remain
    /// unfinished or the scheduler does not consume the source through a real
    /// `None`, even if all declared tasks completed. A failed terminal report
    /// is emitted before the latter error is returned; its failure is retained
    /// as `report_error`.
    ///
    /// # Panics
    ///
    /// Propagates panics from the scheduler, source iterator, and synchronous
    /// reporter callbacks.
    pub fn execute_with_source<I, E, S, Schedule>(
        &self,
        tasks: I,
        count: usize,
        task_failure_policy: TaskFailurePolicy,
        schedule: Schedule,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, S>>
    where
        I: IntoIterator,
        E: Send,
        S: std::error::Error + Send + Sync + 'static,
        Schedule: for<'ctx> FnOnce(
            &mut ParallelBatchSource<'ctx, I, E>,
            &'ctx ParallelBatchExecutionContext<E>,
        ) -> Result<(), S>,
    {
        self.execute_inner(tasks, count, task_failure_policy, |tasks, context| {
            let mut source = ParallelBatchSource::new(tasks, context);
            schedule(&mut source, context)
        })
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between due-based running progress events.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the reporter used for the current coordinator.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub const fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.reporter
    }

    /// Executes a source-aware batch and validates its terminal state.
    fn execute_inner<I, E, S, Schedule>(
        &self,
        tasks: I,
        count: usize,
        task_failure_policy: TaskFailurePolicy,
        schedule: Schedule,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, S>>
    where
        I: IntoIterator,
        E: Send,
        S: std::error::Error + Send + Sync + 'static,
        Schedule: FnOnce(I, &ParallelBatchExecutionContext<E>) -> Result<(), S>,
    {
        let mut progress = match Progress::builder_arc(Arc::clone(&self.reporter))
            .interval(self.report_interval)
            .metric(Metric::new(EXECUTION_PROGRESS_METRIC_ID, EXECUTION_PROGRESS_METRIC_NAME).total(count as u64))
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
        let state = Arc::new(BatchExecutionState::new(count, metric, task_failure_policy));

        let stop_result = thread::scope(|scope| {
            let running_progress = progress.spawn_auto_reporter(scope);
            let context = ParallelBatchExecutionContext::new(
                Arc::clone(&state),
                running_progress.notifier(),
                running_progress.status(),
            );
            let schedule_result = schedule(tasks, &context);
            let observed_count = state.observed_count();
            let accepted_count = state.accepted_count();
            let completed_count = state.completed_count();
            let stop_result = running_progress.stop();
            (
                schedule_result,
                observed_count,
                accepted_count,
                completed_count,
                stop_result,
            )
        });
        let state = Arc::into_inner(state).expect("parallel batch execution state should have a single owner");
        let (schedule_result, observed_count, accepted_count, completed_count, stop_result) = stop_result;
        if let Err(source) = schedule_result {
            let (elapsed, report_error) = match stop_result {
                Ok(()) => Self::fail_progress(progress),
                Err(report_source) => (progress.elapsed(), Some(Box::new(ProgressFailure::from(report_source)))),
            };
            return Err(BatchExecutionError::ScheduleFailed {
                source,
                outcome: state.into_outcome(elapsed),
                report_error,
            });
        }
        if let Err(source) = stop_result {
            return Err(BatchExecutionError::ProgressReport {
                source: Box::new(ProgressFailure::from(source)),
                outcome: state.into_outcome(progress.elapsed()),
            });
        }

        let source_exhaustion_missing = !state.source_exhausted();
        Self::finish(
            progress,
            state,
            count,
            observed_count,
            accepted_count,
            completed_count,
            source_exhaustion_missing,
        )
    }

    /// Returns a zero-completion outcome for immediate setup failures.
    ///
    /// # Type Parameters
    ///
    /// * `E` - Task-specific error type stored in the outcome.
    ///
    /// # Parameters
    ///
    /// * `count` - Declared task count for the empty outcome.
    #[inline]
    fn empty_outcome<E>(count: usize) -> BatchOutcome<E> {
        BatchOutcomeBuilder::builder(count)
            .elapsed(Duration::ZERO)
            .build()
            .expect("empty batch outcome must be valid")
    }

    /// Finalizes completion, reports terminal progress, and maps count errors.
    ///
    /// # Type Parameters
    ///
    /// * `E` - Task-specific error type stored in the outcome.
    /// * `S` - Scheduler error type used by the enclosing operation.
    ///
    /// # Parameters
    ///
    /// * `progress` - Active progress operation to finalize.
    /// * `state` - Shared execution state containing observed task results.
    /// * `count` - Declared task count for the batch.
    /// * `observed_count` - Number of source tasks observed by the scheduler.
    /// * `accepted_count` - Number of tasks accepted by the scheduler.
    /// * `completed_count` - Number of accepted tasks that reached a terminal
    ///   outcome.
    /// * `source_exhaustion_missing` - Whether the scheduler failed to observe
    ///   a real source `None`.
    ///
    /// # Returns
    ///
    /// A validated batch outcome when terminal reporting and count validation
    /// succeed.
    ///
    /// # Errors
    ///
    /// Returns an incomplete-schedule, count-mismatch, or progress-report
    /// error when finalization cannot produce a valid outcome.
    fn finish<E, S>(
        progress: Progress<'_>,
        state: BatchExecutionState<E>,
        count: usize,
        observed_count: usize,
        accepted_count: usize,
        completed_count: usize,
        source_exhaustion_missing: bool,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, S>>
    where
        S: std::error::Error + Send + Sync + 'static,
    {
        if completed_count < accepted_count {
            let (elapsed, report_error) = Self::fail_progress(progress);
            return Err(BatchExecutionError::IncompleteSchedule {
                expected: count,
                accepted: accepted_count,
                observed: observed_count,
                completed: completed_count,
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
        if state.should_stop_accepting() && !state.source_exhausted() {
            let elapsed = match progress.fail() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let elapsed = source.elapsed();
                    return Err(BatchExecutionError::ProgressReport {
                        source: Box::new(ProgressFailure::from(source)),
                        outcome: state.into_outcome_with_termination(
                            elapsed,
                            crate::BatchTermination::StoppedByTaskFailurePolicy,
                        ),
                    });
                }
            };
            return Ok(
                state.into_outcome_with_termination(elapsed, crate::BatchTermination::StoppedByTaskFailurePolicy)
            );
        }
        if observed_count < count {
            let (elapsed, report_error) = Self::fail_progress(progress);
            return Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: observed_count,
                outcome: state.into_outcome(elapsed),
                report_error,
            });
        }
        if source_exhaustion_missing {
            let (elapsed, report_error) = Self::fail_progress(progress);
            return Err(BatchExecutionError::IncompleteSchedule {
                expected: count,
                accepted: accepted_count,
                observed: observed_count,
                completed: completed_count,
                outcome: state.into_outcome(elapsed),
                report_error,
            });
        }

        let terminal = if state.failure_count() > 0 {
            progress
                .fail()
                .map_err(|source| (source.elapsed(), ProgressFailure::from(source)))
        } else {
            progress
                .finish()
                .map_err(|source| (source.elapsed(), ProgressFailure::from_finish_error(source)))
        };

        match terminal {
            Ok(elapsed) => Ok(state.into_outcome(elapsed)),
            Err((elapsed, source)) => Err(BatchExecutionError::ProgressReport {
                source: Box::new(source),
                outcome: state.into_outcome(elapsed),
            }),
        }
    }

    /// Reports a failed terminal phase and returns elapsed with secondary
    /// error.
    ///
    /// # Parameters
    ///
    /// * `progress` - Active progress operation whose terminal phase failed.
    ///
    /// # Returns
    ///
    /// The elapsed duration and an optional secondary progress failure.
    fn fail_progress(progress: Progress<'_>) -> (Duration, Option<Box<ProgressFailure>>) {
        let (elapsed, report_error) = ProgressFailure::fail_operation(progress);
        (elapsed, report_error.map(Box::new))
    }
}
