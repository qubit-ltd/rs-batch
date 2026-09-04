// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic synchronization aliases used by production state and Loom tests.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicBool;
pub(crate) use std::sync::atomic::AtomicU64;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicUsize;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::Ordering;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicBool;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicUsize;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::Ordering;
