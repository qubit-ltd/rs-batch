/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Batch execution state and final outcomes.

mod batch_execution_state;
mod batch_outcome;
mod batch_outcome_build_error;

pub use batch_execution_state::BatchExecutionState;
pub use batch_outcome::BatchOutcome;
pub use batch_outcome_build_error::BatchOutcomeBuildError;
