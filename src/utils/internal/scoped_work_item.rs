// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Indexed work item sent to scoped processor workers.
pub(in crate::utils) struct ScopedWorkItem<T> {
    /// Zero-based item index within the declared batch.
    pub(in crate::utils) index: usize,
    /// Work item payload owned by the receiving worker.
    pub(in crate::utils) item: T,
}
