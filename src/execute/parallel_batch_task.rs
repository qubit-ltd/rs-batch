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
    /// * `index` - Validated zero-based task index.
    /// * `task` - Runnable task payload.
    ///
    /// # Returns
    ///
    /// An execution token owned by the scheduler.
    #[inline]
    pub(crate) const fn new(execution_id: u64, index: usize, task: T) -> Self {
        Self {
            execution_id,
            index,
            task,
        }
    }
}

impl<T> ParallelBatchTask<T> {
    /// Consumes this token and returns its validated execution parts.
    ///
    /// # Returns
    ///
    /// The context-assigned task index and runnable payload.
    #[inline]
    pub(crate) fn into_parts(self) -> (u64, usize, T) {
        (self.execution_id, self.index, self.task)
    }
}
