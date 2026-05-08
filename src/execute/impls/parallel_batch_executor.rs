/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_function::Runnable;
use qubit_progress::{
    Progress,
    reporter::ProgressReporter,
};

use crate::BatchExecutionError;
use crate::BatchOutcome;
use crate::execute::{
    BatchExecutionState,
    BatchExecutor,
    SequentialBatchExecutor,
};
use crate::utils::run_scoped_parallel;

use super::ParallelBatchExecutorBuildError;
use super::ParallelBatchExecutorBuilder;
use super::indexed_task::run_parallel_task;

/// Fixed-width parallel batch executor backed by scoped standard threads.
///
/// The executor creates scoped worker threads for each parallel batch run and
/// shuts them down before [`BatchExecutor::execute`] returns. Because the
/// workers are scoped to the call, tasks may borrow data from the caller and do
/// not need to be `'static`.
///
/// [`Default`] uses [`Self::DEFAULT_SEQUENTIAL_THRESHOLD`], so batches with at
/// most 100 declared tasks run through [`SequentialBatchExecutor`] to avoid
/// thread setup overhead. Configure `sequential_threshold(0)` through
/// [`Self::builder`] when every non-empty batch should use parallel workers.
///
/// ```rust
/// use qubit_batch::{
///     BatchExecutor,
///     ParallelBatchExecutor,
/// };
///
/// let executor = ParallelBatchExecutor::builder()
///     .thread_count(2)
///     .sequential_threshold(0)
///     .build()
///     .expect("parallel executor configuration should be valid");
///
/// let outcome = executor
///     .for_each(0..4, 4, |value| {
///         assert!(value < 4);
///         Ok::<(), &'static str>(())
///     })
///     .expect("range length should match the declared count");
///
/// assert!(outcome.is_success());
/// ```
#[derive(Clone)]
pub struct ParallelBatchExecutor {
    /// Number of worker threads used for parallel executions.
    pub(crate) thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    pub(crate) sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn ProgressReporter>,
}

impl ParallelBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Default maximum batch size that still uses sequential execution.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = 100;

    /// Returns the default worker-thread count.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[inline]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }

    /// Creates a builder for configuring a parallel batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[inline]
    pub fn builder() -> ParallelBatchExecutorBuilder {
        ParallelBatchExecutorBuilder::default()
    }

    /// Creates a parallel batch executor with `thread_count` workers.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Number of scoped worker threads to use.
    ///
    /// # Returns
    ///
    /// A configured parallel batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError::ZeroThreadCount`] when
    /// `thread_count` is zero.
    #[inline]
    pub fn new(thread_count: usize) -> Result<Self, ParallelBatchExecutorBuildError> {
        Self::builder().thread_count(thread_count).build()
    }

    /// Returns the configured worker-thread count.
    ///
    /// # Returns
    ///
    /// The maximum number of scoped worker threads used for one batch.
    #[inline]
    pub const fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum task count that still runs sequentially.
    #[inline]
    pub const fn sequential_threshold(&self) -> usize {
        self.sequential_threshold
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between due-based running progress callbacks.
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

    /// Creates a sequential executor with matching progress configuration.
    ///
    /// # Returns
    ///
    /// A sequential executor used for small batches.
    fn sequential_executor(&self) -> SequentialBatchExecutor {
        SequentialBatchExecutor::new()
            .with_report_interval(self.report_interval)
            .with_reporter_arc(Arc::clone(&self.reporter))
    }
}

impl Default for ParallelBatchExecutor {
    /// Creates a default parallel batch executor.
    ///
    /// # Returns
    ///
    /// A default-configured parallel batch executor.
    ///
    /// # Panics
    ///
    /// Panics if the default configuration fails validation.
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default parallel batch executor should build")
    }
}

impl BatchExecutor for ParallelBatchExecutor {
    /// Executes the batch on scoped standard threads when the batch is large
    /// enough.
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
        E: Send,
    {
        if count <= self.sequential_threshold || self.thread_count <= 1 {
            return self.sequential_executor().execute(tasks, count);
        }

        let state = Arc::new(BatchExecutionState::new(count));
        let progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_started(state.progress_counters());
        let mut actual_count = 0usize;
        let worker_count = self.thread_count.min(count);

        thread::scope(|scope| {
            let reporter_state = Arc::clone(&state);
            let running_progress =
                progress.spawn_running_reporter(scope, move || reporter_state.progress_counters());
            let worker_progress_point_handle = running_progress.point_handle();

            let observer_state = Arc::clone(&state);
            let worker_state = Arc::clone(&state);
            actual_count = run_scoped_parallel(
                tasks,
                count,
                worker_count,
                move || observer_state.record_task_observed(),
                move |index, task| {
                    run_parallel_task(&worker_state, index, task);
                    worker_progress_point_handle.report();
                },
            );
            running_progress.stop_and_join();
        });

        let state = Arc::into_inner(state)
            .expect("parallel batch execution state should have a single owner");
        if actual_count < count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.into_outcome(failed.elapsed());
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: result,
            })
        } else if actual_count > count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.into_outcome(failed.elapsed());
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: actual_count,
                outcome: result,
            })
        } else {
            let finished = progress.report_finished(state.progress_counters());
            let result = state.into_outcome(finished.elapsed());
            Ok(result)
        }
    }
}
