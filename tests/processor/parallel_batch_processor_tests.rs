/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ParallelBatchProcessor`](qubit_batch::ParallelBatchProcessor).

use std::{
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use qubit_function::Consumer;

use qubit_batch::{BatchProcessError, BatchProcessor, ParallelBatchProcessor};

use crate::support::panic_payload_message;

#[test]
fn test_parallel_batch_processor_consumer_accessors() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let processor = ParallelBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    });

    processor.consumer().accept(&5);
    let consumer = processor.into_consumer();
    consumer.accept(&6);

    assert_eq!(
        *accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![5, 6]
    );
}

#[test]
fn test_parallel_batch_processor_processes_items() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = ParallelBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    });

    let result = processor
        .process([1, 2, 3, 4], 4)
        .expect("parallel processing should succeed");
    let mut values = accepted
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    values.sort_unstable();

    assert_eq!(result.item_count(), 4);
    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.processed_count(), 4);
    assert_eq!(result.chunk_count(), 1);
    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(
        processor.thread_count(),
        ParallelBatchProcessor::<i32>::default_thread_count()
    );
}

#[test]
fn test_parallel_batch_processor_accepts_empty_input() {
    let mut processor = ParallelBatchProcessor::new(|_item: &i32| {
        panic!("empty input should not call the consumer");
    });

    let result = processor
        .process([], 0)
        .expect("empty parallel processing should succeed");

    assert_eq!(result.item_count(), 0);
    assert_eq!(result.completed_count(), 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.chunk_count(), 0);
}

#[test]
fn test_parallel_batch_processor_uses_configured_thread_count() {
    let active_count = Arc::new(AtomicUsize::new(0));
    let max_active_count = Arc::new(AtomicUsize::new(0));
    let active_by_consumer = Arc::clone(&active_count);
    let max_by_consumer = Arc::clone(&max_active_count);
    let mut processor = ParallelBatchProcessor::new(move |_item: &usize| {
        let active = active_by_consumer.fetch_add(1, Ordering::AcqRel) + 1;
        update_max(&max_by_consumer, active);
        thread::sleep(Duration::from_millis(20));
        active_by_consumer.fetch_sub(1, Ordering::AcqRel);
    })
    .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));

    let result = processor
        .process(0..6, 6)
        .expect("parallel processing should succeed");

    assert_eq!(processor.thread_count(), 2);
    assert_eq!(result.completed_count(), 6);
    assert!(max_active_count.load(Ordering::Acquire) > 1);
    assert!(max_active_count.load(Ordering::Acquire) <= 2);
}

#[test]
fn test_parallel_batch_processor_supports_non_static_items() {
    let first = AtomicUsize::new(0);
    let second = AtomicUsize::new(0);
    let mut processor = ParallelBatchProcessor::new(|item: &BorrowedItem<'_>| {
        item.counter.fetch_add(1, Ordering::AcqRel);
    })
    .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));
    let items = [
        BorrowedItem { counter: &first },
        BorrowedItem { counter: &second },
    ];

    let result = processor
        .process(items, 2)
        .expect("borrowed items should process");

    assert_eq!(result.processed_count(), 2);
    assert_eq!(first.load(Ordering::Acquire), 1);
    assert_eq!(second.load(Ordering::Acquire), 1);
}

#[test]
fn test_parallel_batch_processor_reports_count_exceeded() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = ParallelBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    })
    .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));

    let error = processor
        .process([1, 2, 3], 2)
        .expect_err("extra input should be reported");

    match error {
        BatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.processed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_processor_reports_count_exceeded_before_first_item() {
    let mut processor = ParallelBatchProcessor::new(|_item: &i32| {
        panic!("excess zero-count input should not call the consumer");
    });

    let error = processor
        .process([1], 0)
        .expect_err("extra input should be reported before any consumer call");

    match error {
        BatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 0);
            assert_eq!(observed_at_least, 1);
            assert_eq!(result.completed_count(), 0);
            assert_eq!(result.processed_count(), 0);
            assert_eq!(result.chunk_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_processor_reports_count_shortfall() {
    let mut processor = ParallelBatchProcessor::new(|_item: &i32| {})
        .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));

    let error = processor
        .process([1, 2], 3)
        .expect_err("short input should be reported");

    match error {
        BatchProcessError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.processed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_parallel_batch_processor_propagates_consumer_panic() {
    const PANIC_MESSAGE: &str = "parallel processor consumer panic";
    let mut processor = ParallelBatchProcessor::new(|_item: &i32| {
        panic!("{PANIC_MESSAGE}");
    })
    .with_thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"));

    let payload = catch_unwind(AssertUnwindSafe(|| processor.process([1], 1)))
        .expect_err("consumer panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

/// Updates `max_value` when `candidate` is larger.
fn update_max(max_value: &AtomicUsize, candidate: usize) {
    let mut current = max_value.load(Ordering::Acquire);
    while candidate > current {
        match max_value.compare_exchange(current, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

/// Test item borrowing a stack-owned counter.
struct BorrowedItem<'a> {
    /// Counter incremented by the processor consumer.
    counter: &'a AtomicUsize,
}
