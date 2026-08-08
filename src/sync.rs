// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic synchronization aliases used by production state and Loom tests.

pub(crate) use std::sync::atomic::AtomicBool;
pub(crate) use std::sync::atomic::AtomicUsize;
pub(crate) use std::sync::atomic::Ordering;
