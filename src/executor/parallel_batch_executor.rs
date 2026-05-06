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
    fmt,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        resume_unwind,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
        mpsc::{
            self,
            RecvTimeoutError,
        },
    },
    thread,
    time::{
        Duration,
        Instant,
    },
};

use qubit_function::Runnable;
use qubit_progress::Progress;
use thiserror::Error;

use crate::{
    BatchExecutionError,
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
    NoOpProgressReporter,
    ProgressCounters,
    ProgressPhase,
    ProgressReporter,
    error::panic_payload_to_error,
};

use super::{
    BatchExecutor,
    SequentialBatchExecutor,
};

/// Error returned when building a [`ParallelBatchExecutor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ParallelBatchExecutorBuildError {
    /// The configured worker-thread count is zero.
    #[error("parallel batch executor thread count must be positive")]
    ZeroThreadCount,

    /// The configured progress-report interval is zero.
    #[error("parallel batch executor report interval must be positive")]
    ZeroReportInterval,
}

/// Builder for [`ParallelBatchExecutor`].
pub struct ParallelBatchExecutorBuilder {
    /// Number of worker threads used for parallel executions.
    num_threads: usize,
    /// Maximum batch size that still uses sequential execution.
    sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl ParallelBatchExecutorBuilder {
    /// Sets the worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `num_threads` - Number of scoped worker threads to use.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the sequential fallback threshold.
    ///
    /// # Parameters
    ///
    /// * `sequential_threshold` - Maximum batch size that still runs
    ///   sequentially.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn sequential_threshold(mut self, sequential_threshold: usize) -> Self {
        self.sequential_threshold = sequential_threshold;
        self
    }

    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum interval between running progress events.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by built executors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Reporter receiving batch lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter<R>(mut self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.reporter = Arc::new(reporter);
        self
    }

    /// Sets the shared progress reporter used by built executors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared reporter receiving batch lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter_arc(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Disables progress callbacks by using [`NoOpProgressReporter`].
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoOpProgressReporter);
        self
    }

    /// Builds a validated [`ParallelBatchExecutor`].
    ///
    /// # Returns
    ///
    /// A parallel batch executor when the configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError`] when the worker count or
    /// report interval is zero.
    pub fn build(self) -> Result<ParallelBatchExecutor, ParallelBatchExecutorBuildError> {
        if self.num_threads == 0 {
            return Err(ParallelBatchExecutorBuildError::ZeroThreadCount);
        }
        if self.report_interval.is_zero() {
            return Err(ParallelBatchExecutorBuildError::ZeroReportInterval);
        }
        Ok(ParallelBatchExecutor {
            num_threads: self.num_threads,
            sequential_threshold: self.sequential_threshold,
            report_interval: self.report_interval,
            reporter: self.reporter,
        })
    }
}

impl Default for ParallelBatchExecutorBuilder {
    /// Creates a builder with default parallel batch settings.
    ///
    /// # Returns
    ///
    /// A builder using available parallelism, five-second progress intervals,
    /// sequential fallback for single-item batches, and no-op reporting.
    fn default() -> Self {
        Self {
            num_threads: ParallelBatchExecutor::default_num_threads(),
            sequential_threshold: ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: ParallelBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }
}

/// Fixed-width parallel batch executor backed by scoped standard threads.
///
/// The executor creates scoped worker threads for each parallel batch run and
/// shuts them down before [`BatchExecutor::execute`] returns. Because the
/// workers are scoped to the call, tasks may borrow data from the caller and do
/// not need to be `'static`.
#[derive(Clone)]
pub struct ParallelBatchExecutor {
    /// Number of worker threads used for parallel executions.
    num_threads: usize,
    /// Maximum batch size that still uses sequential execution.
    sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl ParallelBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Default sequential fallback threshold.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = 1;

    /// Returns the default worker-thread count.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[inline]
    pub fn default_num_threads() -> usize {
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

    /// Creates a parallel batch executor with `num_threads` workers.
    ///
    /// # Parameters
    ///
    /// * `num_threads` - Number of scoped worker threads to use.
    ///
    /// # Returns
    ///
    /// A configured parallel batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError::ZeroThreadCount`] when
    /// `num_threads` is zero.
    #[inline]
    pub fn new(num_threads: usize) -> Result<Self, ParallelBatchExecutorBuildError> {
        Self::builder().num_threads(num_threads).build()
    }

    /// Returns the configured worker-thread count.
    ///
    /// # Returns
    ///
    /// The maximum number of scoped worker threads used for one batch.
    #[inline]
    pub const fn num_threads(&self) -> usize {
        self.num_threads
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
        if count <= self.sequential_threshold || self.num_threads <= 1 {
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
        let worker_count = self.num_threads.min(count);

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
                    .unwrap_or_else(|_| panic!("parallel batch workers should accept tasks"));
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

/// Indexed task submitted to scoped workers.
struct IndexedTask<T> {
    /// Zero-based task index within the batch.
    index: usize,
    /// Task payload.
    task: T,
}

/// Shared progress counters for a running parallel batch.
struct ParallelBatchProgressState {
    /// Number of tasks currently running.
    active_count: AtomicUsize,
    /// Number of tasks that reached a terminal outcome.
    completed_count: AtomicUsize,
    /// Number of successful tasks.
    succeeded_count: AtomicUsize,
    /// Number of failed tasks.
    failed_count: AtomicUsize,
    /// Number of panicked tasks.
    panicked_count: AtomicUsize,
}

impl ParallelBatchProgressState {
    /// Creates fresh progress state for one batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters.
    fn new() -> Self {
        Self {
            active_count: AtomicUsize::new(0),
            completed_count: AtomicUsize::new(0),
            succeeded_count: AtomicUsize::new(0),
            failed_count: AtomicUsize::new(0),
            panicked_count: AtomicUsize::new(0),
        }
    }

    /// Returns the number of completed tasks.
    ///
    /// # Returns
    ///
    /// The completed task counter.
    fn completed_count(&self) -> usize {
        self.completed_count.load(Ordering::Acquire)
    }

    /// Builds generic progress counters from current state.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared total task count.
    ///
    /// # Returns
    ///
    /// Progress counters suitable for reporter events.
    fn progress_counters(&self, total_count: usize) -> ProgressCounters {
        ProgressCounters::new(Some(total_count))
            .with_active_count(self.active_count.load(Ordering::Acquire))
            .with_completed_count(self.completed_count.load(Ordering::Acquire))
            .with_succeeded_count(self.succeeded_count.load(Ordering::Acquire))
            .with_failed_count(
                self.failed_count.load(Ordering::Acquire)
                    + self.panicked_count.load(Ordering::Acquire),
            )
    }

    /// Records that one task has started.
    fn record_task_started(&self) {
        self.active_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one successful task completion.
    fn record_task_succeeded(&self) {
        self.active_count.fetch_sub(1, Ordering::AcqRel);
        self.completed_count.fetch_add(1, Ordering::AcqRel);
        self.succeeded_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one task error.
    fn record_task_failed(&self) {
        self.active_count.fetch_sub(1, Ordering::AcqRel);
        self.completed_count.fetch_add(1, Ordering::AcqRel);
        self.failed_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one task panic.
    fn record_task_panicked(&self) {
        self.active_count.fetch_sub(1, Ordering::AcqRel);
        self.completed_count.fetch_add(1, Ordering::AcqRel);
        self.panicked_count.fetch_add(1, Ordering::AcqRel);
    }
}

/// Shared result counters and failure storage for a running parallel batch.
struct ParallelBatchResultState<E> {
    /// Number of successful tasks.
    succeeded_count: AtomicUsize,
    /// Number of failed tasks.
    failed_count: AtomicUsize,
    /// Number of panicked tasks.
    panicked_count: AtomicUsize,
    /// Detailed task failure list.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> ParallelBatchResultState<E> {
    /// Creates fresh result state for one batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters and no recorded failures.
    fn new() -> Self {
        Self {
            succeeded_count: AtomicUsize::new(0),
            failed_count: AtomicUsize::new(0),
            panicked_count: AtomicUsize::new(0),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Records one successful task.
    fn record_task_succeeded(&self) {
        self.succeeded_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one task error.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Task error returned by the task.
    fn record_task_failed(&self, index: usize, error: E) {
        self.failed_count.fetch_add(1, Ordering::AcqRel);
        lock_failures(&self.failures)
            .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
    }

    /// Records one task panic.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based task index.
    /// * `error` - Captured task panic.
    fn record_task_panicked(&self, index: usize, error: BatchTaskError<E>) {
        self.panicked_count.fetch_add(1, Ordering::AcqRel);
        lock_failures(&self.failures).push(BatchTaskFailure::new(index, error));
    }

    /// Builds a structured batch result from collected state.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared batch task count.
    /// * `completed_count` - Number of tasks completed by workers.
    /// * `elapsed` - Total elapsed wall-clock time.
    ///
    /// # Returns
    ///
    /// A structured batch execution result.
    fn into_outcome(
        self,
        task_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) -> BatchOutcome<E> {
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchOutcomeBuilder::builder(task_count)
            .completed_count(completed_count)
            .succeeded_count(self.succeeded_count.load(Ordering::Acquire))
            .failed_count(self.failed_count.load(Ordering::Acquire))
            .panicked_count(self.panicked_count.load(Ordering::Acquire))
            .elapsed(elapsed)
            .failures(failures)
            .build()
            .expect("parallel batch executor should collect consistent counters")
    }
}

/// Runs tasks from a shared receiver until the channel closes.
///
/// # Parameters
///
/// * `task_receiver` - Shared receiver protected because standard receivers are
///   not `Sync`.
/// * `progress_state` - Shared progress counters.
/// * `result_state` - Shared final result state.
fn run_parallel_worker<T, E>(
    task_receiver: Arc<Mutex<mpsc::Receiver<IndexedTask<T>>>>,
    progress_state: Arc<ParallelBatchProgressState>,
    result_state: Arc<ParallelBatchResultState<E>>,
) where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    loop {
        let received = task_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        let Ok(indexed_task) = received else {
            break;
        };
        run_parallel_task(&progress_state, &result_state, indexed_task);
    }
}

/// Runs one task and records its outcome.
///
/// # Parameters
///
/// * `progress_state` - Shared progress counters.
/// * `result_state` - Shared final result state.
/// * `indexed_task` - Indexed task to execute.
fn run_parallel_task<T, E>(
    progress_state: &ParallelBatchProgressState,
    result_state: &ParallelBatchResultState<E>,
    indexed_task: IndexedTask<T>,
) where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    let IndexedTask { index, mut task } = indexed_task;
    progress_state.record_task_started();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    match outcome {
        Ok(Ok(())) => {
            progress_state.record_task_succeeded();
            result_state.record_task_succeeded();
        }
        Ok(Err(error)) => {
            progress_state.record_task_failed();
            result_state.record_task_failed(index, error);
        }
        Err(payload) => {
            progress_state.record_task_panicked();
            result_state.record_task_panicked(index, panic_payload_to_error(payload.as_ref()));
        }
    }
}

/// Runs the periodic progress loop for one parallel batch execution.
///
/// # Parameters
///
/// * `reporter` - Reporter receiving progress callbacks.
/// * `state` - Shared batch state read by the reporting loop.
/// * `total_count` - Declared task count for the batch.
/// * `start` - Batch start time.
/// * `report_interval` - Delay between progress callbacks.
/// * `stop_receiver` - Stop signal receiver used by the caller thread.
fn run_progress_loop(
    reporter: Arc<dyn ProgressReporter>,
    state: Arc<ParallelBatchProgressState>,
    total_count: usize,
    start: Instant,
    report_interval: Duration,
    stop_receiver: mpsc::Receiver<()>,
) {
    let progress = Progress::from_start(reporter.as_ref(), report_interval, start);
    loop {
        match stop_receiver.recv_timeout(report_interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => progress.report_with_elapsed(
                ProgressPhase::Running,
                state.progress_counters(total_count),
                start.elapsed(),
            ),
        }
    }
}

/// Locks a failure list, recovering from poisoning.
///
/// # Parameters
///
/// * `failures` - Failure list mutex to lock.
///
/// # Returns
///
/// A guard for the failure list.
fn lock_failures<E>(
    failures: &Mutex<Vec<BatchTaskFailure<E>>>,
) -> std::sync::MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
    failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Builds progress counters from a completed batch outcome.
///
/// # Parameters
///
/// * `outcome` - Completed or partial batch outcome.
///
/// # Returns
///
/// Progress counters suitable for terminal reporter events.
fn outcome_progress_counters<E>(outcome: &BatchOutcome<E>) -> ProgressCounters {
    ProgressCounters::new(Some(outcome.task_count()))
        .with_completed_count(outcome.completed_count())
        .with_succeeded_count(outcome.succeeded_count())
        .with_failed_count(outcome.failure_count())
}
