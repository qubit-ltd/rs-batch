// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

/// Controls when an executor stops admitting source tasks after failures.
///
/// A failure is either a task-returned error or a task panic captured by the
/// executor. Progress-reporting and task-source errors are batch-level errors
/// and do not contribute to this policy.
/// In parallel execution the policy stops accepting new source items, while
/// already accepted task tokens are allowed to finish.
///
/// # Author
///
/// Haixing Hu
///
/// # Examples
///
/// ```rust
/// use std::num::NonZeroUsize;
/// use qubit_batch::TaskFailurePolicy;
/// let policy = TaskFailurePolicy::StopAfterFailures(NonZeroUsize::new(2).expect("positive limit"));
/// assert!(!policy.should_stop(1));
/// assert!(policy.should_stop(2));
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum TaskFailurePolicy {
    /// Continues through every task and collects all task failures.
    #[default]
    Continue,
    /// Stops after the first task error or captured task panic.
    StopOnFirstFailure,
    /// Stops after the configured number of task failures.
    StopAfterFailures(
        /// Positive number of failed or panicked tasks that stops admission.
        NonZeroUsize,
    ),
}

impl TaskFailurePolicy {
    /// Returns whether execution should stop at `failure_count`.
    ///
    /// # Parameters
    ///
    /// * `failure_count` - Number of task errors and captured panics observed
    ///   so far.
    ///
    /// # Returns
    ///
    /// `true` when the policy has reached its stopping condition; otherwise
    /// `false`.
    #[must_use]
    #[inline]
    pub const fn should_stop(self, failure_count: usize) -> bool {
        match self {
            Self::StopOnFirstFailure => failure_count >= 1,
            Self::Continue => false,
            Self::StopAfterFailures(limit) => failure_count >= limit.get(),
        }
    }
}
