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
    time::Duration,
};

use crate::{
    BatchOutcomeBuilder,
    BatchTaskFailure,
};

/// Final or partial outcome produced by one batch execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOutcome<E> {
    /// Declared task count for this batch.
    task_count: usize,
    /// Number of tasks that reached a terminal outcome.
    completed_count: usize,
    /// Number of tasks that completed successfully.
    succeeded_count: usize,
    /// Number of tasks that returned their own error.
    failed_count: usize,
    /// Number of tasks that panicked.
    panicked_count: usize,
    /// Total monotonic elapsed duration for the batch.
    elapsed: Duration,
    /// Detailed failure records sorted by task index.
    failures: Vec<BatchTaskFailure<E>>,
}

impl<E> BatchOutcome<E> {
    /// Creates a new batch outcome from a validated builder.
    ///
    /// # Parameters
    ///
    /// * `builder` - Validated outcome builder carrying all outcome fields.
    ///
    /// # Returns
    ///
    /// A fully populated batch outcome.
    pub fn new(builder: BatchOutcomeBuilder<E>) -> Self {
        Self {
            task_count: builder.task_count,
            completed_count: builder.completed_count,
            succeeded_count: builder.succeeded_count,
            failed_count: builder.failed_count,
            panicked_count: builder.panicked_count,
            elapsed: builder.elapsed,
            failures: builder.failures,
        }
    }

    /// Returns the declared task count for this batch.
    ///
    /// # Returns
    ///
    /// The expected number of tasks supplied by the caller.
    pub const fn task_count(&self) -> usize {
        self.task_count
    }

    /// Returns how many tasks reached a terminal outcome.
    ///
    /// # Returns
    ///
    /// The number of completed tasks.
    pub const fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Returns how many tasks completed successfully.
    ///
    /// # Returns
    ///
    /// The number of successful tasks.
    pub const fn succeeded_count(&self) -> usize {
        self.succeeded_count
    }

    /// Returns how many tasks returned their own error.
    ///
    /// # Returns
    ///
    /// The number of failed tasks.
    pub const fn failed_count(&self) -> usize {
        self.failed_count
    }

    /// Returns how many tasks panicked.
    ///
    /// # Returns
    ///
    /// The number of panicked tasks.
    pub const fn panicked_count(&self) -> usize {
        self.panicked_count
    }

    /// Returns the total number of task failures.
    ///
    /// # Returns
    ///
    /// Failed plus panicked task count.
    pub const fn failure_count(&self) -> usize {
        self.failed_count + self.panicked_count
    }

    /// Returns the total monotonic elapsed duration.
    ///
    /// # Returns
    ///
    /// The elapsed duration for this batch execution.
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns the detailed failure records collected during execution.
    ///
    /// # Returns
    ///
    /// A shared slice of task failure records.
    pub fn failures(&self) -> &[BatchTaskFailure<E>] {
        self.failures.as_slice()
    }

    /// Returns whether every task completed successfully.
    ///
    /// # Returns
    ///
    /// `true` if the batch has no failures and every declared task completed.
    pub const fn is_success(&self) -> bool {
        self.completed_count == self.task_count
            && self.failed_count == 0
            && self.panicked_count == 0
    }

    /// Consumes this outcome and returns its failure list.
    ///
    /// # Returns
    ///
    /// The detailed failure records collected during execution.
    pub fn into_failures(self) -> Vec<BatchTaskFailure<E>> {
        self.failures
    }
}

impl<E> fmt::Display for BatchOutcome<E> {
    /// Formats a concise summary of this batch outcome.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Formatter receiving the summary text.
    ///
    /// # Returns
    ///
    /// The formatting result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "BatchOutcome {{ task_count: {}, completed_count: {}, succeeded_count: {}, failed_count: {}, panicked_count: {}, elapsed: {:?} }}",
            self.task_count(),
            self.completed_count(),
            self.succeeded_count(),
            self.failed_count(),
            self.panicked_count(),
            self.elapsed(),
        )
    }
}
