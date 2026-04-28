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
        resume_unwind,
    },
    sync::{
        Arc,
        Mutex,
        mpsc::{
            self,
            Receiver,
            RecvTimeoutError,
        },
    },
    thread,
    time::{
        Duration,
        Instant,
    },
};

use qubit_atomic::AtomicCount;
use qubit_function::Runnable;
use rayon::{
    ThreadPool as RayonThreadPool,
    ThreadPoolBuildError as RayonThreadPoolBuildError,
};
use thiserror::Error;

use crate::{
    BatchExecutionError,
    BatchExecutionResult,
    BatchTaskError,
    BatchTaskFailure,
    error::panic_payload_to_error,
    progress::{
        NoOpProgressReporter,
        ProgressReporter,
    },
};

use super::{
    BatchExecutor,
    ParallelBatchExecutorBuilder,
    SequentialBatchExecutor,
};

/// Default worker-thread name prefix for [`ParallelBatchExecutor`].
const DEFAULT_THREAD_NAME_PREFIX: &str = "qubit-parallel-batch";

/// Error returned when a [`ParallelBatchExecutor`] cannot be built.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug, Error)]
pub enum ParallelBatchExecutorBuildError {
    /// The configured parallelism is zero.
    #[error("parallel batch executor parallelism must be greater than zero")]
    ZeroParallelism,

    /// The configured worker stack size is zero.
    #[error("parallel batch executor worker stack size must be greater than zero")]
    ZeroStackSize,

    /// The configured progress-report interval is zero.
    #[error("parallel batch executor report interval must be greater than zero")]
    ZeroReportInterval,

    /// Rayon rejected the underlying thread-pool configuration.
    #[error("failed to build parallel batch executor: {source}")]
    BuildFailed {
        /// Underlying Rayon thread-pool build error.
        #[from]
        source: RayonThreadPoolBuildError,
    },
}

/// Parallel batch executor backed by a dedicated Rayon thread pool.
///
/// The executor runs small batches sequentially when the declared batch size is
/// at or below the configured threshold.
///
/// # Author
///
/// Haixing Hu
#[derive(Clone)]
pub struct ParallelBatchExecutor {
    /// Dedicated Rayon pool used for parallel batch execution.
    pool: Arc<RayonThreadPool>,
    /// Number of Rayon worker threads configured for this executor.
    parallelism: usize,
    /// Maximum batch size that still uses sequential execution.
    parallel_threshold: usize,
    /// Interval between progress callbacks while a batch is running.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl ParallelBatchExecutor {
    /// Default parallelism used by the builder.
    pub fn default_parallelism() -> usize {
        thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }

    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Default sequential fallback threshold.
    pub const DEFAULT_PARALLEL_THRESHOLD: usize = 1;

    /// Creates a builder for configuring a parallel batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default Rayon settings.
    #[inline]
    pub fn builder() -> ParallelBatchExecutorBuilder {
        ParallelBatchExecutorBuilder::default()
    }

    /// Creates a parallel batch executor with the supplied parallelism.
    ///
    /// # Parameters
    ///
    /// * `parallelism` - Number of Rayon worker threads to create.
    ///
    /// # Returns
    ///
    /// A configured parallel batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError`] when the supplied
    /// configuration is invalid or Rayon rejects it.
    #[inline]
    pub fn new(parallelism: usize) -> Result<Self, ParallelBatchExecutorBuildError> {
        Self::builder().parallelism(parallelism).build()
    }

    /// Returns the configured Rayon worker-thread count.
    ///
    /// # Returns
    ///
    /// The configured parallelism level.
    #[inline]
    pub const fn parallelism(&self) -> usize {
        self.parallelism
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum task count that still uses sequential execution.
    #[inline]
    pub const fn parallel_threshold(&self) -> usize {
        self.parallel_threshold
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between progress callbacks.
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

    /// Builds a parallel batch executor from validated configuration.
    ///
    /// # Parameters
    ///
    /// * `parallelism` - Number of Rayon worker threads to create.
    /// * `parallel_threshold` - Sequential fallback threshold.
    /// * `report_interval` - Minimum interval between progress callbacks.
    /// * `reporter` - Reporter receiving batch lifecycle callbacks.
    /// * `thread_name_prefix` - Prefix used when naming Rayon workers.
    /// * `stack_size` - Optional worker stack size in bytes.
    ///
    /// # Returns
    ///
    /// A configured parallel batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelBatchExecutorBuildError`] when the supplied
    /// configuration is invalid or Rayon rejects it.
    pub(crate) fn from_parts(
        parallelism: usize,
        parallel_threshold: usize,
        report_interval: Duration,
        reporter: Arc<dyn ProgressReporter>,
        thread_name_prefix: String,
        stack_size: Option<usize>,
    ) -> Result<Self, ParallelBatchExecutorBuildError> {
        if parallelism == 0 {
            return Err(ParallelBatchExecutorBuildError::ZeroParallelism);
        }
        if report_interval.is_zero() {
            return Err(ParallelBatchExecutorBuildError::ZeroReportInterval);
        }
        if stack_size == Some(0) {
            return Err(ParallelBatchExecutorBuildError::ZeroStackSize);
        }
        let prefix = thread_name_prefix;
        let mut builder = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism)
            .thread_name(move |index| format!("{prefix}-{index}"));
        if let Some(stack_size) = stack_size {
            builder = builder.stack_size(stack_size);
        }
        let pool = Arc::new(builder.build()?);
        Ok(Self {
            pool,
            parallelism,
            parallel_threshold,
            report_interval,
            reporter,
        })
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
    /// Panics if Rayon rejects the default thread-pool configuration.
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default parallel batch executor should build")
    }
}

impl BatchExecutor for ParallelBatchExecutor {
    /// Executes the batch on Rayon workers when the batch is large enough.
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
        if count <= self.parallel_threshold || self.parallelism <= 1 {
            let sequential = SequentialBatchExecutor::new()
                .with_report_interval(self.report_interval)
                .with_reporter_arc(Arc::clone(&self.reporter));
            return sequential.execute(tasks, count);
        }

        self.reporter.start(count);
        let progress_state = Arc::new(ParallelBatchProgressState::new());
        let result_state = Arc::new(ParallelBatchResultState::new());
        let start = Instant::now();
        let reporter = Arc::clone(&self.reporter);
        let reporter_state = Arc::clone(&progress_state);
        let report_interval = self.report_interval;
        let (stop_sender, stop_receiver) = mpsc::channel();
        let progress_thread = thread::spawn(move || {
            run_progress_loop(
                reporter,
                reporter_state,
                count,
                start,
                report_interval,
                stop_receiver,
            );
        });

        let mut actual_count = 0usize;
        self.pool.in_place_scope_fifo(|scope| {
            for task in tasks {
                if actual_count == count {
                    actual_count += 1;
                    break;
                }
                let index = actual_count;
                actual_count += 1;
                let task_progress_state = Arc::clone(&progress_state);
                let task_result_state = Arc::clone(&result_state);
                scope.spawn_fifo(move |_| {
                    run_parallel_task(task_progress_state, task_result_state, index, task);
                });
            }
        });

        let _ = stop_sender.send(());
        if let Err(payload) = progress_thread.join() {
            resume_unwind(payload);
        }

        let completed_count = progress_state.completed_count.get();
        let result = Arc::into_inner(result_state)
            .expect("parallel batch result state should have a single owner")
            .into_result(count, completed_count, start.elapsed());
        self.reporter.finish(count, result.elapsed());
        if actual_count < count {
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                result,
            })
        } else if actual_count > count {
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: actual_count,
                result,
            })
        } else {
            Ok(result)
        }
    }
}

/// Shared progress counters for a running parallel batch.
struct ParallelBatchProgressState {
    /// Number of tasks currently running on worker threads.
    active_count: AtomicCount,
    /// Number of completed tasks.
    completed_count: AtomicCount,
}

impl ParallelBatchProgressState {
    /// Creates fresh progress state for one parallel batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters.
    fn new() -> Self {
        Self {
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
        }
    }
}

/// Shared result counters and failure storage for a running parallel batch.
struct ParallelBatchResultState<E> {
    /// Number of successful tasks.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
    /// Detailed task failure list.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> ParallelBatchResultState<E> {
    /// Creates fresh result state for one parallel batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters and no recorded failures.
    fn new() -> Self {
        Self {
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Builds a structured batch result from the collected counters.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared batch task count.
    /// * `elapsed` - Total elapsed wall-clock time.
    ///
    /// # Returns
    ///
    /// A structured batch execution result.
    fn into_result(
        self,
        task_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) -> BatchExecutionResult<E> {
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchExecutionResult::from_validated_parts(
            task_count,
            completed_count,
            self.succeeded_count.get(),
            self.failed_count.get(),
            self.panicked_count.get(),
            elapsed,
            failures,
        )
    }
}

/// Executes one task on a Rayon worker and updates shared statistics.
///
/// # Parameters
///
/// * `state` - Shared batch state updated for the task outcome.
/// * `index` - Zero-based task index within the batch.
/// * `task` - Runnable task executed on the current Rayon worker.
fn run_parallel_task<T, E>(
    progress_state: Arc<ParallelBatchProgressState>,
    result_state: Arc<ParallelBatchResultState<E>>,
    index: usize,
    mut task: T,
) where
    T: Runnable<E>,
    E: Send,
{
    progress_state.active_count.inc();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    progress_state.active_count.dec();
    match outcome {
        Ok(Ok(())) => {
            progress_state.completed_count.inc();
            result_state.succeeded_count.inc();
        }
        Ok(Err(error)) => {
            progress_state.completed_count.inc();
            result_state.failed_count.inc();
            lock_failures(&result_state.failures)
                .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
        }
        Err(payload) => {
            progress_state.completed_count.inc();
            result_state.panicked_count.inc();
            lock_failures(&result_state.failures).push(BatchTaskFailure::new(
                index,
                panic_payload_to_error(payload.as_ref()),
            ));
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
fn run_progress_loop(
    reporter: Arc<dyn ProgressReporter>,
    state: Arc<ParallelBatchProgressState>,
    total_count: usize,
    start: Instant,
    report_interval: Duration,
    stop_receiver: Receiver<()>,
) {
    loop {
        match stop_receiver.recv_timeout(report_interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                let completed_count = state.completed_count.get();
                let active_count = state.active_count.get();
                reporter.process(total_count, active_count, completed_count, start.elapsed());
            }
        }
    }
}

/// Acquires the failure list while tolerating poisoned mutexes.
///
/// # Parameters
///
/// * `failures` - Mutex protecting the detailed failure list.
///
/// # Returns
///
/// A mutex guard for the failure list.
fn lock_failures<E>(
    failures: &Mutex<Vec<BatchTaskFailure<E>>>,
) -> std::sync::MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
    failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Returns the default Rayon worker-thread name prefix.
///
/// # Returns
///
/// The default worker-thread prefix used by the builder.
pub(crate) fn default_thread_name_prefix() -> String {
    DEFAULT_THREAD_NAME_PREFIX.to_owned()
}

/// Returns the default progress reporter for parallel execution.
///
/// # Returns
///
/// A shared no-op progress reporter.
pub(crate) fn default_reporter() -> Arc<dyn ProgressReporter> {
    Arc::new(NoOpProgressReporter)
}
