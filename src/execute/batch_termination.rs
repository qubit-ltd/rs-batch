// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Describes how a batch executor stopped consuming its task source.
///
/// A [`BatchTermination::StoppedByTaskFailurePolicy`] outcome stops admission
/// before source exhaustion is observed. More items may remain, so the declared
/// count has not been fully validated. `Finished` means execution was not
/// stopped by this policy; it does not imply that every task succeeded.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::BatchTermination;
/// use qubit_batch::SequentialBatchExecutor;
/// use qubit_batch::TaskFailurePolicy;
/// let outcome = SequentialBatchExecutor::builder()
///     .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure).build()
///     .execute_with_count(std::iter::repeat_with(|| || Err::<(), _>("failed")), 10)
///     .expect("policy stops return an outcome");
/// assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchTermination {
    /// Execution finished without a task-failure-policy short circuit.
    #[default]
    Finished,
    /// Execution stopped after the configured task failure policy was reached.
    StoppedByTaskFailurePolicy,
}
