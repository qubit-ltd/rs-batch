// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Terminal outcome status for one executable task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskExecutionStatus {
    /// Task reached success.
    Succeeded,
    /// Task failed or panicked.
    Failed,
}
