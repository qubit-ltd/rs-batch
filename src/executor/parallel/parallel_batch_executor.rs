/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::fmt;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_function::Runnable;
use qubit_progress::Progress;

use crate::BatchExecutionError;
use crate::BatchOutcome;
use crate::ProgressCounters;
use crate::ProgressPhase;
use crate::ProgressReporter;

use crate::executor::BatchExecutor;
use crate::executor::SequentialBatchExecutor;

use super::ParallelBatchExecutorBuildError;
use super::ParallelBatchExecutorBuilder;
use super::indexed_task::IndexedTask;
use super::indexed_task::run_parallel_worker;
use super::parallel_batch_progress_state::ParallelBatchProgressState;
use super::parallel_batch_progress_state::run_progress_loop;
use super::parallel_batch_result_state::ParallelBatchResultState;

/// Fixed-width parallel batch executor backed by scoped standard threads.
///
/// The executor creates scoped worker threads for each parallel batch run and
/// shuts them down before [`BatchExecutor::execute`] returns. Because the
/// workers are scoped to the call, tasks may borrow data from the caller and do
/// not need to be `'static`.
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

    /// Default sequential fallback threshold.
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
    /// The minimum interval between running progress callbacks.
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
        E: Send + fmt::Debug,
    {
        if count <= self.sequential_threshold || self.thread_count <= 1 {
            return self.sequential_executor().execute(tasks, count);
        }

        let progress_state = Arc::new(ParallelBatchProgressState::new());
        let result_state = Arc::new(ParallelBatchResultState::new());
        let progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            progress_state.progress_counters(count),
            Duration::ZERO,
        );
        let start = progress.started_at();
        let mut actual_count = 0usize;
        let worker_count = self.thread_count.min(count);

        thread::scope(|scope| {
            let (stop_sender, stop_receiver) = mpsc::channel();
            let progress_handle = {
                let progress_reporter = Arc::clone(&self.reporter);
                let reporter_state = Arc::clone(&progress_state);
                let report_interval = self.report_interval;
                scope.spawn(move || {
                    run_progress_loop(
                        progress_reporter,
                        reporter_state,
                        count,
                        start,
                        report_interval,
                        stop_receiver,
                    );
                })
            };

            let (task_sender, task_receiver) = mpsc::sync_channel(worker_count);
            let task_receiver = Arc::new(Mutex::new(task_receiver));
            let mut worker_handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let worker_receiver = Arc::clone(&task_receiver);
                let worker_progress_state = Arc::clone(&progress_state);
                let worker_result_state = Arc::clone(&result_state);
                worker_handles.push(scope.spawn(move || {
                    run_parallel_worker(
                        worker_receiver,
                        worker_progress_state,
                        worker_result_state,
                    );
                }));
            }

            for task in tasks {
                if actual_count == count {
                    actual_count += 1;
                    break;
                }
                let index = actual_count;
                actual_count += 1;
                task_sender
                    .send(IndexedTask { index, task })
                    .expect("parallel batch workers should accept tasks");
            }
            drop(task_sender);

            for handle in worker_handles {
                if let Err(payload) = handle.join() {
                    resume_unwind(payload);
                }
            }
            let _ = stop_sender.send(());
            if let Err(payload) = progress_handle.join() {
                resume_unwind(payload);
            }
        });

        let completed_count = progress_state.completed_count();
        let elapsed = progress.elapsed();
        let result = Arc::into_inner(result_state)
            .expect("parallel batch result state should have a single owner")
            .into_outcome(count, completed_count, elapsed);

        if actual_count < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: result,
            })
        } else if actual_count > count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: actual_count,
                outcome: result,
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Ok(result)
        }
    }
}

fn outcome_progress_counters<E>(outcome: &BatchOutcome<E>) -> ProgressCounters {
    ProgressCounters::new(Some(outcome.task_count()))
        .with_completed_count(outcome.completed_count())
        .with_succeeded_count(outcome.succeeded_count())
        .with_failed_count(outcome.failure_count())
}
