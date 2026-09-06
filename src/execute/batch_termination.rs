// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Describes how a batch executor stopped consuming its task source.
///
/// A [`BatchTermination::StoppedByTaskFailurePolicy`] outcome leaves the
/// remaining source items unconsumed, so an explicit declared count was not
/// fully validated. `Finished` means execution was not stopped by this policy;
/// it does not imply that every task succeeded.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchTermination {
    /// Execution finished without a task-failure-policy short circuit.
    #[default]
    Finished,
    /// Execution stopped after the configured task failure policy was reached.
    StoppedByTaskFailurePolicy,
}
