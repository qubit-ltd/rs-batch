// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Carries one batch item and its position to a scoped processor worker.
///
/// # Type Parameters
///
/// * `T` - The payload type owned by the worker processing this item.
pub(in crate::utils) struct ScopedWorkItem<T> {
    /// Zero-based position of this item in the declared batch.
    pub(in crate::utils) index: usize,
    /// Work item payload owned by the receiving worker.
    pub(in crate::utils) item: T,
}
