// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// A task accepted by a parallel batch execution context.
///
/// The context creates this token only after assigning a unique, in-range
/// index. The token is consumed by
/// [`crate::execute::spi::ParallelBatchExecutionContext::execute_task`] and
/// cannot be constructed or reused by a scheduler.
///
/// # Type Parameters
///
/// * `T` - Task payload that implements `Runnable<E>` for the target execution.
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
/// let outcome = coordinator.execute([|| Ok::<(), &'static str>(())], 1,
///     TaskFailurePolicy::Continue, |tasks, context| {
///         let mut source = tasks.into_iter();
///         while let Some(token) = context.next_task(&mut source) {
///             context.execute_task(token);
///         }
///         Ok::<(), Infallible>(())
///     }).expect("all accepted tasks finish before the scheduler returns");
/// assert!(outcome.is_success());
/// ```
#[must_use = "accepted parallel batch tasks must be executed"]
pub struct ParallelBatchTask<T> {
    /// Globally unique execution-context identity.
    pub(crate) execution_id: u64,
    /// Zero-based index assigned by the execution context.
    pub(crate) index: usize,
    /// Runnable payload accepted for execution.
    pub(crate) task: T,
}

impl<T> ParallelBatchTask<T> {
    /// Creates a task token after the context validates its index.
    ///
    /// # Parameters
    ///
    /// * `execution_id` - Identity of the coordinator run that accepted the
    ///   task.
    /// * `index` - Validated zero-based task index.
    /// * `task` - Runnable task payload.
    ///
    /// # Returns
    ///
    /// An execution token owned by the scheduler.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub(crate) const fn new(execution_id: u64, index: usize, task: T) -> Self {
        Self {
            execution_id,
            index,
            task,
        }
    }

    /// Consumes this token and returns its validated execution parts.
    ///
    /// # Returns
    ///
    /// The execution identity, context-assigned task index, and runnable
    /// payload.
    #[inline]
    pub(crate) fn into_parts(self) -> (u64, usize, T) {
        (self.execution_id, self.index, self.task)
    }
}
