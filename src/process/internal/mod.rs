// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Implementation state and adapters owned by process.

mod batch_process_state;

pub(crate) use batch_process_state::BatchProcessState;
pub(crate) use batch_process_state::PROCESS_PROGRESS_METRIC_ID;
pub(crate) use batch_process_state::PROCESS_PROGRESS_METRIC_NAME;
