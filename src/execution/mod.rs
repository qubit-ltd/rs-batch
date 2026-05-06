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
//!
//! The mutable execution state is crate-internal. External callers should use
//! [`BatchOutcomeBuilder`] when they need to construct outcomes manually.
//!
//! ```compile_fail
//! use qubit_batch::execution::BatchExecutionState;
//! ```

mod batch_execution_state;
mod batch_outcome;
mod batch_outcome_build_error;
mod batch_outcome_builder;

pub(crate) use batch_execution_state::BatchExecutionState;
pub use batch_outcome::BatchOutcome;
pub use batch_outcome_build_error::BatchOutcomeBuildError;
pub use batch_outcome_builder::BatchOutcomeBuilder;
