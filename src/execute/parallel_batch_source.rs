// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::iter::FusedIterator;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::ParallelBatchExecutionContext;
use super::ParallelBatchTask;

/// Scheduler-owned source that centralizes lazy iteration and task admission.
///
/// The source converts the user iterator only when its first item is needed,
/// records source exhaustion exactly once, and permanently stops after an
/// admission gate rejects an item. Runtime integrations should consume this
/// iterator instead of manually pairing an input iterator with
/// `ParallelBatchExecutionContext::next_task`.
///
/// # Type Parameters
///
/// * `'ctx` - Lifetime of the execution context borrowed by the scheduler.
/// * `I` - Source converted lazily into an iterator of runnable tasks.
/// * `E` - Task-specific error stored in the shared outcome.
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
///
/// let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
/// let outcome = coordinator.execute_with_source([|| Ok::<(), &'static str>(())], 1,
///     TaskFailurePolicy::Continue, |source, context| {
///         for token in source {
///             context.execute_task(token);
///         }
///         Ok::<(), Infallible>(())
///     }).expect("the source is exhausted and its accepted task completed");
/// assert!(outcome.is_success());
/// ```
pub struct ParallelBatchSource<'ctx, I: IntoIterator, E> {
    /// User-provided source retained until the first item is requested.
    input: Option<I>,
    /// Lazily initialized iterator created from [`Self::input`].
    iterator: Option<I::IntoIter>,
    /// Execution context that validates and accepts source items.
    context: &'ctx ParallelBatchExecutionContext<E>,
    /// Whether this source has permanently stopped producing items.
    done: bool,
    /// Shared marker used by the coordinator to verify source exhaustion.
    exhausted: Arc<AtomicBool>,
}

impl<'ctx, I: IntoIterator, E> ParallelBatchSource<'ctx, I, E> {
    /// Creates a source for one coordinator invocation.
    pub(crate) fn with_exhaustion(
        input: I,
        context: &'ctx ParallelBatchExecutionContext<E>,
        exhausted: Arc<AtomicBool>,
    ) -> Self {
        Self {
            input: Some(input),
            iterator: None,
            context,
            done: false,
            exhausted,
        }
    }
}

impl<I: IntoIterator, E> Iterator for ParallelBatchSource<'_, I, E> {
    type Item = ParallelBatchTask<I::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.done || !self.context.is_accepting() {
            self.done = true;
            self.iterator.take();
            return None;
        }
        if self.iterator.is_none() {
            let input = self.input.take().expect("parallel source input is converted once");
            self.iterator = Some(input.into_iter());
        }
        let token = self
            .context
            .next_task(self.iterator.as_mut().expect("parallel source iterator is initialized"));
        if token.is_none() {
            self.done = true;
            self.iterator.take();
            if self.context.source_exhausted() {
                self.exhausted.store(true, Ordering::Release);
            }
        }
        token
    }
}

impl<I: IntoIterator, E> FusedIterator for ParallelBatchSource<'_, I, E> {}
