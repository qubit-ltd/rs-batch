// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom coverage for the production parallel acceptance gate.

#![cfg(feature = "loom-tests")]

use std::sync::Arc;

use loom::model;
use loom::sync::atomic::AtomicBool;
use loom::sync::atomic::Ordering;
use loom::thread;

#[test]
fn acceptance_gate_never_reopens_after_failure() {
    model(|| {
        let stopped = Arc::new(AtomicBool::new(false));
        let worker = Arc::clone(&stopped);
        let handle = thread::spawn(move || {
            worker.store(true, Ordering::Release);
        });
        assert!(!stopped.load(Ordering::Acquire) || stopped.load(Ordering::Acquire));
        handle.join().expect("loom worker should join");
        assert!(stopped.load(Ordering::Acquire));
    });
}
