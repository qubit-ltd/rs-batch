// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom models using the production admission state.

use loom::model;
use loom::sync::Arc;
use loom::thread;

use crate::execute::internal::ParallelBatchAcceptanceState;

#[test]
fn test_loom_stop_races_with_admission_without_reopening() {
    model(|| {
        let state = Arc::new(ParallelBatchAcceptanceState::new(2));
        let producer_state = Arc::clone(&state);
        let producer = thread::spawn(move || {
            let observed = producer_state.try_record_observed();
            if observed.is_some() {
                producer_state.record_accepted();
            }
            observed
        });
        let stopper_state = Arc::clone(&state);
        let stopper = thread::spawn(move || stopper_state.stop());
        let observed = producer.join().expect("loom producer should join");
        stopper.join().expect("loom stopper should join");
        assert_eq!(state.accepted_count(), usize::from(observed.is_some()));
        assert!(state.try_record_observed().is_none());
        assert!(state.should_stop());
    });
}

#[test]
fn test_loom_concurrent_admission_preserves_each_atomic_counter() {
    model(|| {
        let state = Arc::new(ParallelBatchAcceptanceState::new(2));
        let first = Arc::clone(&state);
        let second = Arc::clone(&state);
        let first_handle = thread::spawn(move || {
            let observed = first.record_observed();
            first.record_accepted();
            observed
        });
        let second_handle = thread::spawn(move || {
            let observed = second.record_observed();
            second.record_accepted();
            observed
        });
        let mut observations = [
            first_handle.join().expect("first loom worker should join"),
            second_handle.join().expect("second loom worker should join"),
        ];
        observations.sort_unstable();
        assert_eq!(observations, [1, 2]);
        assert_eq!(state.observed_count(), 2);
        assert_eq!(state.accepted_count(), 2);
    });
}

#[test]
fn test_loom_source_exhaustion_races_with_policy_stop_without_being_lost() {
    model(|| {
        let state = Arc::new(ParallelBatchAcceptanceState::new(1));
        let exhausted = Arc::clone(&state);
        let stopper = Arc::clone(&state);
        let source = thread::spawn(move || exhausted.mark_source_exhausted());
        let policy = thread::spawn(move || stopper.stop());
        source.join().expect("loom source should join");
        policy.join().expect("loom policy should join");
        assert!(state.source_exhausted());
        assert!(state.should_stop());
        assert!(state.try_record_observed().is_none());
    });
}

#[test]
fn test_loom_policy_stop_without_source_none_does_not_claim_exhaustion() {
    model(|| {
        let state = Arc::new(ParallelBatchAcceptanceState::new(1));
        let stopper = Arc::clone(&state);
        let policy = thread::spawn(move || stopper.stop());
        policy.join().expect("loom policy should join");
        assert!(state.should_stop());
        assert!(!state.source_exhausted());
    });
}
