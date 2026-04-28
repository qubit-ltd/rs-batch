/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use qubit_function::Runnable;

use crate::{
    BatchExecutionError,
    BatchExecutionResult,
    BatchTaskError,
    BatchTaskFailure,
    batch_task_error::panic_payload_to_error,
    progress::{
        NoOpProgressReporter,
        ProgressReporter,
    },
};

use super::BatchExecutor;

/// Executes a whole batch sequentially on the caller thread.
///
/// Progress updates are emitted only between tasks. A long-running single task
/// therefore does not produce intermediate sequential progress callbacks.
///
/// # Author
///
/// Haixing Hu
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
    ) -> Result<BatchExecutionResult<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        self.reporter.start(count);
        let start = Instant::now();
        let mut next_progress = start + self.report_interval;
        let mut completed_count = 0;
        let mut succeeded_count = 0;
        let mut failed_count = 0;
        let mut panicked_count = 0;
        let mut failures = Vec::new();
        let mut actual_count = 0;
        for task in tasks {
            if actual_count == count {
                let result = build_result(
                    count,
                    completed_count,
                    succeeded_count,
                    failed_count,
                    panicked_count,
                    start.elapsed(),
                    failures,
                );
                self.reporter.finish(count, result.elapsed());
                return Err(BatchExecutionError::CountExceeded {
                    expected: count,
                    observed_at_least: count + 1,
                    result,
                });
            }
            execute_one_task(
                task,
                actual_count,
                &mut completed_count,
                &mut succeeded_count,
                &mut failed_count,
                &mut panicked_count,
                &mut failures,
            );
            actual_count += 1;
            maybe_report_progress(
                self.reporter.as_ref(),
                count,
                completed_count,
                start,
                self.report_interval,
                &mut next_progress,
            );
        }

        let result = build_result(
            count,
            completed_count,
            succeeded_count,
            failed_count,
            panicked_count,
            start.elapsed(),
            failures,
        );
        self.reporter.finish(count, result.elapsed());
        if actual_count < count {
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                result,
            })
        } else {
            Ok(result)
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
fn execute_one_task<T, E>(
    mut task: T,
    index: usize,
    completed_count: &mut usize,
    succeeded_count: &mut usize,
    failed_count: &mut usize,
    panicked_count: &mut usize,
    failures: &mut Vec<BatchTaskFailure<E>>,
) where
    T: Runnable<E>,
{
    match catch_unwind(AssertUnwindSafe(|| task.run())) {
        Ok(Ok(())) => {
            *completed_count += 1;
            *succeeded_count += 1;
        }
        Ok(Err(error)) => {
            *completed_count += 1;
            *failed_count += 1;
            failures.push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
        }
        Err(payload) => {
            *completed_count += 1;
            *panicked_count += 1;
            failures.push(BatchTaskFailure::new(
                index,
                panic_payload_to_error(payload.as_ref()),
            ));
        }
    }
}

/// Emits a periodic progress callback for sequential execution.
///
/// # Parameters
///
/// * `reporter` - Progress reporter receiving the callback.
/// * `total_count` - Declared batch task count.
/// * `completed_count` - Number of tasks that have completed.
/// * `start` - Batch start time.
/// * `next_progress` - Deadline for the next progress callback.
fn maybe_report_progress(
    reporter: &dyn ProgressReporter,
    total_count: usize,
    completed_count: usize,
    start: Instant,
    report_interval: Duration,
    next_progress: &mut Instant,
) {
    let now = Instant::now();
    if now < *next_progress {
        return;
    }
    reporter.process(total_count, 0, completed_count, now.duration_since(start));
    *next_progress = now + report_interval;
}

/// Builds a batch execution result from sequential counters.
///
/// # Parameters
///
/// * `task_count` - Declared batch task count.
/// * `completed_count` - Number of completed tasks.
/// * `succeeded_count` - Number of successful tasks.
/// * `failed_count` - Number of failed tasks.
/// * `panicked_count` - Number of panicked tasks.
/// * `elapsed` - Total elapsed wall-clock time.
/// * `failures` - Detailed failure list.
///
/// # Returns
///
/// A structured batch execution result.
fn build_result<E>(
    task_count: usize,
    completed_count: usize,
    succeeded_count: usize,
    failed_count: usize,
    panicked_count: usize,
    elapsed: Duration,
    failures: Vec<BatchTaskFailure<E>>,
) -> BatchExecutionResult<E> {
    BatchExecutionResult::from_validated_parts(
        task_count,
        completed_count,
        succeeded_count,
        failed_count,
        panicked_count,
        elapsed,
        failures,
    )
}
