/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Internal batch execution and processing state.

mod batch_counter;
mod batch_execution_state;
mod batch_process_state;

pub(crate) use batch_counter::BatchCounter;
pub(crate) use batch_execution_state::BatchExecutionState;
pub(crate) use batch_process_state::BatchProcessState;
