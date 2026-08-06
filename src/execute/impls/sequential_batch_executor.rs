// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{sync::Arc, time::Duration};

use qubit_function::Runnable;
use qubit_progress::{Metric, Progress, Reporter};

use crate::{
    BatchExecutionError, BatchOutcome, BatchOutcomeBuilder, BatchTermination, ProgressFailure,
    TaskFailurePolicy,
    execute::{
        BatchExecutionState, BatchExecutor, EXECUTION_PROGRESS_METRIC_ID,
        EXECUTION_PROGRESS_METRIC_NAME, TaskExecutionStatus,
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
    pub(crate) reporter: Arc<dyn Reporter>,
    /// Policy applied after task errors or captured task panics.
    pub(crate) task_failure_policy: TaskFailurePolicy,
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
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.reporter
    }

    /// Returns the configured task failure policy.
    ///
    /// # Returns
    ///
    /// The policy that controls whether sequential execution stops after task
    /// errors or captured task panics.
    #[inline]
    pub const fn task_failure_policy(&self) -> TaskFailurePolicy {
        self.task_failure_policy
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
    /// Panics from tasks are captured in the result. Panics from synchronous
    /// progress reporter callbacks are propagated to the caller.
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
        let mut progress = match Progress::builder_arc(Arc::clone(&self.reporter))
            .interval(self.report_interval)
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
                    outcome: BatchOutcomeBuilder::builder(count)
                        .elapsed(Duration::ZERO)
                        .build()
                        .expect("empty batch outcome must be valid"),
                });
            }
        };
        let metric = progress
            .metric(EXECUTION_PROGRESS_METRIC_ID)
            .expect("configured execution metric must exist");
        let state = BatchExecutionState::new(count, metric);
        let mut actual_count = 0;
        let mut stopped_by_task_failure_policy = false;
        let mut failure_count = 0usize;
        for task in tasks {
            actual_count = state.record_task_observed();
            if actual_count > count {
                let (elapsed, report_error) = match progress.fail() {
                    Ok(elapsed) => (elapsed, None),
                    Err(source) => (
                        source.elapsed(),
                        Some(Box::new(ProgressFailure::from(source))),
                    ),
                };
                let outcome = state.into_outcome(elapsed);
                return Err(BatchExecutionError::CountExceeded {
                    expected: count,
                    observed_at_least: actual_count,
                    outcome,
                    report_error,
                });
            }
            // Execute the task and update the state.
            match state
                .execute_task(actual_count - 1, task)
                .expect("observed task index must be within the declared count")
            {
                TaskExecutionStatus::Succeeded => {}
                TaskExecutionStatus::Failed => failure_count += 1,
            }
            if self.task_failure_policy.should_stop(failure_count) {
                stopped_by_task_failure_policy = true;
                break;
            }
            // Update the actual task count and report progress if due.
            if let Err(source) = progress.report_if_due() {
                let elapsed = progress.elapsed();
                return Err(BatchExecutionError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    outcome: state.into_outcome(elapsed),
                });
            }
        }

        if stopped_by_task_failure_policy {
            let elapsed = match progress.fail() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let elapsed = source.elapsed();
                    return Err(BatchExecutionError::ProgressReport {
                        source: Box::new(ProgressFailure::from(source)),
                        outcome: state.into_outcome_with_termination(
                            elapsed,
                            BatchTermination::StoppedByTaskFailurePolicy,
                        ),
                    });
                }
            };
            Ok(state.into_outcome_with_termination(
                elapsed,
                BatchTermination::StoppedByTaskFailurePolicy,
            ))
        } else if actual_count < count {
            let (elapsed, report_error) = match progress.fail() {
                Ok(elapsed) => (elapsed, None),
                Err(source) => (
                    source.elapsed(),
                    Some(Box::new(ProgressFailure::from(source))),
                ),
            };
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: state.into_outcome(elapsed),
                report_error,
            })
        } else if failure_count > 0 {
            let elapsed = match progress.fail() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let elapsed = source.elapsed();
                    return Err(BatchExecutionError::ProgressReport {
                        source: Box::new(ProgressFailure::from(source)),
                        outcome: state.into_outcome(elapsed),
                    });
                }
            };
            Ok(state.into_outcome(elapsed))
        } else {
            let elapsed = match progress.finish() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let elapsed = source.elapsed();
                    let failure = ProgressFailure::from_finish_error(source);
                    return Err(BatchExecutionError::ProgressReport {
                        source: Box::new(failure),
                        outcome: state.into_outcome(elapsed),
                    });
                }
            };
            Ok(state.into_outcome(elapsed))
        }
    }
}
