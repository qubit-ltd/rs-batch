// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_function::Runnable;
use qubit_progress::Reporter;

use super::ParallelBatchExecutorBuildError;
use super::ParallelBatchExecutorBuilder;
use crate::BatchExecutionError;
use crate::BatchOutcome;
use crate::TaskFailurePolicy;
use crate::execute::BatchExecutor;
use crate::execute::ParallelBatchExecutionCoordinator;
use crate::execute::SequentialBatchExecutor;
use crate::utils::run_scoped_parallel_tasks;

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
/// [`Self::builder`] with more than one worker when every non-empty batch
/// should use the parallel path.
/// The default threshold is a fixed heuristic; benchmark representative task
/// workloads before changing it for a deployment.
///
/// # Examples
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
///     .for_each(0..4, |value| {
///         assert!(value < 4);
///         Ok::<(), &'static str>(())
///     })
///     .expect("range length should be exact");
///
/// assert!(outcome.is_success());
/// ```
#[derive(Clone)]
pub struct ParallelBatchExecutor {
    /// Number of worker threads used for parallel executions.
    pub(crate) thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    pub(crate) sequential_threshold: usize,
    /// Shared coordinator used for parallel execution flow.
    pub(crate) coordinator: ParallelBatchExecutionCoordinator,
    /// Policy applied after task failures in parallel workers.
    pub(crate) task_failure_policy: TaskFailurePolicy,
}

impl ParallelBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = crate::constants::DEFAULT_REPORT_INTERVAL;

    /// Default maximum batch size that still uses sequential execution.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = crate::constants::DEFAULT_SEQUENTIAL_THRESHOLD;

    /// Creates a builder for configuring a parallel batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
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
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
    pub fn new(thread_count: usize) -> Result<Self, ParallelBatchExecutorBuildError> {
        Self::builder().thread_count(thread_count).build()
    }

    /// Returns the default worker-thread count.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[must_use = "use the constructed or borrowed value"]
    #[inline(always)]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism().map(usize::from).unwrap_or(1)
    }

    /// Returns the configured worker-thread count.
    ///
    /// # Returns
    ///
    /// The maximum number of scoped worker threads used for one batch.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum task count that still runs sequentially.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn sequential_threshold(&self) -> usize {
        self.sequential_threshold
    }

    /// Returns the configured task-failure policy.
    ///
    /// # Returns
    ///
    /// The policy applied after task errors or captured task panics.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn task_failure_policy(&self) -> TaskFailurePolicy {
        self.task_failure_policy
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between due-based running progress callbacks.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn report_interval(&self) -> Duration {
        self.coordinator.report_interval()
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        self.coordinator.reporter()
    }

    /// Creates a sequential executor with matching progress configuration.
    ///
    /// # Returns
    ///
    /// A sequential executor used for small batches.
    fn sequential_executor(&self) -> SequentialBatchExecutor {
        SequentialBatchExecutor::builder()
            .report_interval(self.report_interval())
            .reporter_arc(Arc::clone(self.reporter()))
            .task_failure_policy(self.task_failure_policy)
            .build()
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
    /// Standard scoped scheduling has no recoverable submission error.
    type SchedulerError = Infallible;
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
    /// A structured batch result when reporting and count validation succeed,
    /// or a batch-level error with the attached partial result.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when reporting fails, or
    /// a count-mismatch variant when `tasks` yields fewer or more tasks than
    /// `count`.
    ///
    /// # Panics
    ///
    /// Panics from tasks are captured in the result. Panics from synchronous
    /// progress callbacks are propagated to the caller; panics from the
    /// scoped running reporter are returned as [`crate::ProgressFailure`].
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
        // Workers are intentionally scoped to this call rather than shared
        // across calls. This preserves borrowed-task semantics and makes the
        // executor's lifetime boundary explicit; use the sequential fallback
        // for small batches instead of treating worker creation as reusable
        // pool state.
        if count <= self.sequential_threshold || self.thread_count <= 1 {
            return self.sequential_executor().execute_with_count(tasks, count);
        }

        let worker_count = self.thread_count.min(count);
        self.coordinator
            .execute(tasks, count, self.task_failure_policy, move |tasks, context| {
                let mut tasks = tasks.into_iter();
                run_scoped_parallel_tasks(
                    std::iter::from_fn(|| context.next_task(&mut tasks)),
                    worker_count,
                    Some,
                    |task| context.execute_task(task),
                );
                Ok::<(), Infallible>(())
            })
    }
}
