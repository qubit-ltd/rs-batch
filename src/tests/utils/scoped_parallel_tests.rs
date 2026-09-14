// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for source admission after a processing stop signal.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::utils::run_scoped_parallel;

#[test]
fn test_scoped_processing_does_not_pull_next_source_item_after_stop() {
    let pulls = AtomicUsize::new(0);
    let stopped = AtomicBool::new(false);
    let source = std::iter::from_fn(|| {
        pulls.fetch_add(1, Ordering::Relaxed);
        Some(1)
    });

    run_scoped_parallel(
        source,
        3,
        1,
        || {
            stopped.store(true, Ordering::Release);
            1
        },
        || stopped.load(Ordering::Acquire),
        |_, _| {},
    );

    assert_eq!(pulls.load(Ordering::Relaxed), 1);
}
