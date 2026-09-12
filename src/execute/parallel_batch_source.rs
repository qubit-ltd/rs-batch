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
pub struct ParallelBatchSource<'ctx, I: IntoIterator, E> {
    input: Option<I>,
    iterator: Option<I::IntoIter>,
    context: &'ctx ParallelBatchExecutionContext<E>,
    done: bool,
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
