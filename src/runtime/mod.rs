/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Internal runtime helpers shared by executors and processors.

mod scoped_parallel_runner;

pub(crate) use scoped_parallel_runner::run_scoped_parallel;
