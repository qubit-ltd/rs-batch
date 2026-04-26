/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Progress reporting types for batch execution.
//!
//! # Author
//!
//! Haixing Hu

mod no_op_progress_reporter;
mod progress_reporter;

pub use no_op_progress_reporter::NoOpProgressReporter;
pub use progress_reporter::ProgressReporter;
