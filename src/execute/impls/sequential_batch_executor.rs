// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::time::Duration;

use qubit_function::Callable;
use qubit_function::Runnable;
use qubit_progress::Metric;
use qubit_progress::MetricError;
use qubit_progress::Progress;
use qubit_progress::Reporter;

use super::SequentialBatchExecutorBuilder;
use crate::BatchCallOutput;
use crate::BatchExecutionError;
use crate::BatchOutcome;
use crate::BatchOutcomeBuilder;
use crate::BatchTermination;
use crate::ProgressFailure;
use crate::TaskFailurePolicy;
use crate::execute::BatchCallError;
use crate::execute::BatchCallResult;
use crate::execute::BatchExecutionState;
use crate::execute::BatchExecutor;
use crate::execute::EXECUTION_PROGRESS_METRIC_ID;
use crate::execute::EXECUTION_PROGRESS_METRIC_NAME;
use crate::execute::TaskExecutionStatus;

/// Executes a whole batch sequentially on the caller thread.
///
/// Progress updates are emitted only between tasks. A long-running single task
/// therefore does not produce intermediate sequential progress callbacks.
///
/// # Examples
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
    pub const DEFAULT_REPORT_INTERVAL: Duration = crate::constants::DEFAULT_REPORT_INTERVAL;

    /// Creates a sequential batch executor with default configuration.
    ///
    /// # Returns
    ///
    /// A sequential batch executor using no-op progress reporting.
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a builder for configuring a sequential batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
    pub fn builder() -> SequentialBatchExecutorBuilder {
        SequentialBatchExecutorBuilder::default()
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between due-based running progress callbacks.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.reporter
    }

    /// Returns the configured task failure policy.
    ///
    /// # Returns
    ///
    /// The policy that controls whether sequential execution stops after task
    /// errors or captured task panics.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
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
    #[inline(always)]
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SequentialBatchExecutor {
    /// Executes tasks sequentially using the exact count reported by `tasks`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    /// * `E` - Task-specific error type.
    /// * `I` - Exact-size task source type.
    ///
    /// This concrete API accepts non-`Send` tasks because execution remains on
    /// the caller thread.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Exact-size task source.
    ///
    /// # Returns
    ///
    /// The validated batch outcome, or a count/progress error.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when progress reporting fails or the
    /// source violates its exact-size contract.
    pub fn execute<T, E, I>(&self, tasks: I) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
        T: Runnable<E>,
    {
        let tasks = tasks.into_iter();
        let count = tasks.len();
        self.execute_inner(tasks, count)
    }

    /// Executes tasks sequentially with an explicit declared count.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    /// * `E` - Task-specific error type.
    /// * `I` - Task source type.
    ///
    /// Unlike [`BatchExecutor::execute_with_count`], this concrete method does
    /// not require task or error types to be `Send`.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source consumed on the caller thread.
    /// * `count` - Declared number of tasks expected from `tasks`.
    ///
    /// # Returns
    ///
    /// The batch outcome, or a count/progress error.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when progress reporting fails or the
    /// source count differs from `count`.
    #[inline(always)]
    pub fn execute_with_count<T, E, I>(&self, tasks: I, count: usize) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E>,
    {
        self.execute_inner(tasks, count)
    }

    /// Executes callable tasks sequentially using their exact iterator count.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type.
    /// * `R` - Callable success value type.
    /// * `E` - Callable error type.
    /// * `I` - Exact-size callable source type.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Exact-size callable source.
    ///
    /// # Returns
    ///
    /// A callable result containing indexed success values.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallError`] when execution reports progress or count
    /// failures.
    pub fn call<C, R, E, I>(&self, tasks: I) -> Result<BatchCallResult<R, E>, BatchCallError<R, E>>
    where
        I: IntoIterator<Item = C>,
        I::IntoIter: ExactSizeIterator,
        C: Callable<R, E>,
    {
        let tasks = tasks.into_iter();
        let count = tasks.len();
        self.call_with_count(tasks, count)
    }

    /// Executes callable tasks sequentially with an explicit declared count.
    ///
    /// The concrete sequential API accepts non-`Send` callables, values, and
    /// errors because no worker thread is created.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type.
    /// * `R` - Callable success value type.
    /// * `E` - Callable error type.
    /// * `I` - Callable source type.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Callable source consumed on the caller thread.
    /// * `count` - Declared number of callables expected from `tasks`.
    ///
    /// # Returns
    ///
    /// A callable result with success values indexed by callable position.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallError`] when execution reports progress or count
    /// failures.
    pub fn call_with_count<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchCallError<R, E>>
    where
        I: IntoIterator<Item = C>,
        C: Callable<R, E>,
    {
        let mut outputs = Vec::new();
        let execution = self.execute_items_inner(tasks, count, |state, index, callable| {
            let outputs_ref = &mut outputs;
            state.execute_action(index, move || {
                let mut callable = callable;
                let value = callable.call()?;
                drop(callable);
                outputs_ref.push(BatchCallOutput::new(index, value));
                Ok(())
            })
        });
        match execution {
            Ok(outcome) => {
                Ok(BatchCallResult::try_new(outcome, outputs).expect("sequential outputs must match successful tasks"))
            }
            Err(source) => Err(BatchCallError::new(source, outputs)),
        }
    }

    /// Applies a fallible action sequentially using the exact item count.
    ///
    /// # Type Parameters
    ///
    /// * `Item` - Input item type.
    /// * `E` - Action error type.
    /// * `I` - Exact-size item source type.
    /// * `F` - Fallible item-action type.
    ///
    /// # Parameters
    ///
    /// * `items` - Exact-size item source.
    /// * `action` - Fallible action applied to each item.
    ///
    /// # Returns
    ///
    /// The resulting batch outcome.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when execution reports progress or
    /// count failures.
    ///
    /// # Panics
    ///
    /// Panics from `action` are captured as task failures. Iterator and
    /// synchronous reporter callback panics are propagated. Automatic reporter
    /// failures are returned as [`BatchExecutionError::ProgressReport`].
    pub fn for_each<Item, E, I, F>(&self, items: I, action: F) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = Item>,
        I::IntoIter: ExactSizeIterator,
        F: FnMut(Item) -> Result<(), E>,
    {
        let items = items.into_iter();
        let count = items.len();
        self.for_each_with_count(items, count, action)
    }

    /// Applies a fallible action sequentially with an explicit item count.
    ///
    /// # Type Parameters
    ///
    /// * `Item` - Input item type.
    /// * `E` - Action error type.
    /// * `I` - Item source type.
    /// * `F` - Fallible item-action type.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source consumed on the caller thread.
    /// * `count` - Declared number of items expected from `items`.
    /// * `action` - Fallible action applied to each item.
    ///
    /// # Returns
    ///
    /// The resulting batch outcome.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when execution reports progress or the
    /// source count differs from `count`.
    ///
    /// # Panics
    ///
    /// Panics from `action` are captured as task failures. Iterator and
    /// synchronous reporter callback panics are propagated. Automatic reporter
    /// failures are returned as [`BatchExecutionError::ProgressReport`].
    pub fn for_each_with_count<Item, E, I, F>(
        &self,
        items: I,
        count: usize,
        mut action: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = Item>,
        F: FnMut(Item) -> Result<(), E>,
    {
        self.execute_items_inner(items, count, |state, index, item| {
            state.execute_action(index, || action(item))
        })
    }

    /// Executes items sequentially on the caller thread.
    ///
    /// # Type Parameters
    ///
    /// * `Item` - Input item type.
    /// * `E` - Task error type.
    /// * `I` - Item source type.
    /// * `F` - Indexed item runner type.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared item count expected from `items`.
    /// * `run_item` - Callback that executes one indexed item.
    ///
    /// # Returns
    ///
    /// A structured batch result when the declared task count matches, or a
    /// batch-count mismatch error with the attached partial result.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when progress reporting
    /// fails, or a count-mismatch error when `items` yields fewer or more items
    /// than `count`.
    ///
    /// # Panics
    ///
    /// Panics from `run_item` may be captured by the callback itself. Iterator
    /// and synchronous progress reporter panics are propagated to the caller.
    fn execute_items_inner<Item, E, I, F>(
        &self,
        items: I,
        count: usize,
        mut run_item: F,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = Item>,
        F: FnMut(&BatchExecutionState<E>, usize, Item) -> Result<TaskExecutionStatus, MetricError>,
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
        let state = BatchExecutionState::new(count, metric, self.task_failure_policy);
        let mut actual_count = 0;
        let mut stopped_by_task_failure_policy = false;
        let mut failure_count = 0usize;
        for item in items {
            actual_count = state.record_task_observed();
            if actual_count > count {
                let (elapsed, report_error) = {
                    let (elapsed, report_error) = ProgressFailure::fail_operation(progress);
                    (elapsed, report_error.map(Box::new))
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
            match run_item(&state, actual_count - 1, item)
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
                        outcome: state
                            .into_outcome_with_termination(elapsed, BatchTermination::StoppedByTaskFailurePolicy),
                    });
                }
            };
            Ok(state.into_outcome_with_termination(elapsed, BatchTermination::StoppedByTaskFailurePolicy))
        } else if actual_count < count {
            let (elapsed, report_error) = {
                let (elapsed, report_error) = ProgressFailure::fail_operation(progress);
                (elapsed, report_error.map(Box::new))
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

    /// Executes the batch sequentially on the caller thread.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    /// * `E` - Task error type.
    /// * `I` - Task source type.
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
    /// Returns [`BatchExecutionError`] when progress reporting fails or
    /// `tasks` yields fewer or more tasks than `count`.
    ///
    /// # Panics
    ///
    /// Panics from tasks are captured in the result. Iterator, task destructor,
    /// and synchronous progress reporter panics are propagated to the caller.
    #[inline(always)]
    fn execute_inner<T, E, I>(&self, tasks: I, count: usize) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E>,
    {
        self.execute_items_inner(tasks, count, |state, index, task| state.execute_task(index, task))
    }
}

impl BatchExecutor for SequentialBatchExecutor {
    /// Caller-thread execution cannot reject a runtime submission.
    type SchedulerError = std::convert::Infallible;
    /// Executes the batch sequentially on the caller thread.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    /// * `E` - Task error type.
    /// * `I` - Task source type.
    #[inline(always)]
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        SequentialBatchExecutor::execute_with_count(self, tasks, count)
    }

    /// Executes callables through the direct sequential collection path.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type.
    /// * `R` - Callable success value type.
    /// * `E` - Callable error type.
    /// * `I` - Callable source type.
    #[inline(always)]
    fn call_with_count<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchCallError<R, E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = C>,
        C: Callable<R, E> + Send,
        R: Send,
        E: Send,
    {
        SequentialBatchExecutor::call_with_count(self, tasks, count)
    }
}
