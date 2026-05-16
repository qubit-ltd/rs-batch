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
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Arc,
        Mutex,
    },
    thread,
    time::Duration,
};

use qubit_atomic::{
    ArcAtomic,
    ArcAtomicCount,
    AtomicCount,
};
use qubit_batch::{
    BatchProcessError,
    BatchProcessor,
    ParallelBatchExecutor,
    ParallelBatchProcessor,
};
use qubit_function::Consumer;

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
    panic_payload_message,
};

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
fn test_parallel_batch_processor_accessors_and_value_reporter() {
    let processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .reporter(RecordingProgressReporter::new())
        .sequential_threshold(7)
        .report_interval(Duration::from_millis(25))
        .build();
    let no_reporter_processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .no_reporter()
        .build();

    assert_eq!(processor.report_interval(), Duration::from_millis(25));
    assert_eq!(processor.sequential_threshold(), 7);
    assert!(Arc::strong_count(processor.reporter()) >= 1);
    assert_eq!(
        no_reporter_processor.report_interval(),
        ParallelBatchProcessor::<i32>::DEFAULT_REPORT_INTERVAL
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
        .process_with_count(vec![1, 2, 3, 4], 4)
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
    assert_eq!(
        ParallelBatchProcessor::<i32>::DEFAULT_SEQUENTIAL_THRESHOLD,
        ParallelBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(
        processor.sequential_threshold(),
        ParallelBatchProcessor::<i32>::DEFAULT_SEQUENTIAL_THRESHOLD
    );
}

#[test]
fn test_parallel_batch_processor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {
        thread::sleep(Duration::from_millis(20));
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .sequential_threshold(0)
    .reporter_arc(reporter.clone())
    .report_interval(Duration::from_millis(5))
    .build();

    let result = processor
        .process_with_count(vec![1, 2, 3, 4], 4)
        .expect("parallel processing should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 4);
    assert!(matches!(
        events.first(),
        Some(ProgressEvent::Start { total_count: 4 })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 4,
            active_count,
            ..
        } if *active_count > 0
    )));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish {
            total_count: 4,
            completed_count: 4,
        })
    ));
}

#[test]
fn test_parallel_batch_processor_reports_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
        .sequential_threshold(0)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::ZERO)
        .build();

    let result = processor
        .process_with_count(vec![1, 2, 3], 3)
        .expect("parallel processing should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 3);
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 3,
            completed_count,
            ..
        } if *completed_count >= 1
    )));
}

#[test]
fn test_parallel_batch_processor_accepts_empty_input() {
    let mut processor = ParallelBatchProcessor::new(|_item: &i32| {
        panic!("empty input should not call the consumer");
    });

    let result = processor
        .process_with_count(Vec::<i32>::new(), 0)
        .expect("empty parallel processing should succeed");

    assert_eq!(result.item_count(), 0);
    assert_eq!(result.completed_count(), 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.chunk_count(), 0);
}

#[test]
fn test_parallel_batch_processor_uses_configured_thread_count() {
    let active_count = ArcAtomicCount::zero();
    let max_active_count = ArcAtomic::new(0usize);
    let active_by_consumer = active_count.clone();
    let max_by_consumer = max_active_count.clone();
    let mut processor = ParallelBatchProcessor::builder(move |_item: &i32| {
        let active = active_by_consumer.inc();
        max_by_consumer.fetch_max(active);
        thread::sleep(Duration::from_millis(20));
        active_by_consumer.dec();
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .sequential_threshold(0)
    .build();

    let result = processor
        .process_with_count(vec![0, 1, 2, 3, 4, 5], 6)
        .expect("parallel processing should succeed");

    assert_eq!(processor.thread_count(), 2);
    assert_eq!(processor.sequential_threshold(), 0);
    assert_eq!(result.completed_count(), 6);
    assert!(max_active_count.load() > 1);
    assert!(max_active_count.load() <= 2);
}

#[test]
fn test_parallel_batch_processor_uses_sequential_threshold() {
    let active_count = ArcAtomicCount::zero();
    let max_active_count = ArcAtomic::new(0usize);
    let active_by_consumer = active_count.clone();
    let max_by_consumer = max_active_count.clone();
    let mut processor = ParallelBatchProcessor::builder(move |_item: &i32| {
        let active = active_by_consumer.inc();
        max_by_consumer.fetch_max(active);
        thread::sleep(Duration::from_millis(1));
        active_by_consumer.dec();
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .build();

    let result = processor
        .process_with_count(vec![0, 1, 2, 3, 4, 5], 6)
        .expect("small batch should process through the sequential fallback");

    assert_eq!(
        processor.sequential_threshold(),
        ParallelBatchProcessor::<i32>::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(result.completed_count(), 6);
    assert_eq!(max_active_count.load(), 1);
}

#[test]
fn test_parallel_batch_processor_supports_non_static_items() {
    let first = AtomicCount::zero();
    let second = AtomicCount::zero();
    let mut processor = ParallelBatchProcessor::builder(|item: &BorrowedItem<'_>| {
        item.counter.inc();
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .sequential_threshold(0)
    .build();
    let items = [
        BorrowedItem { counter: &first },
        BorrowedItem { counter: &second },
    ];

    let result = processor
        .process_with_count(items, 2)
        .expect("borrowed items should process");

    assert_eq!(result.processed_count(), 2);
    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 1);
}

#[test]
fn test_parallel_batch_processor_reports_count_exceeded() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = ParallelBatchProcessor::builder(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .build();

    let error = processor
        .process_with_count(vec![1, 2, 3], 2)
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
        .process_with_count(vec![1], 0)
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
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
        .build();

    let error = processor
        .process_with_count(vec![1, 2], 3)
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
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {
        panic!("{PANIC_MESSAGE}");
    })
    .thread_count(NonZeroUsize::new(2).expect("thread count is non-zero"))
    .build();

    let payload = catch_unwind(AssertUnwindSafe(|| {
        processor.process_with_count(vec![1], 1)
    }))
    .expect_err("consumer panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_parallel_batch_processor_propagates_worker_panic_after_channel_backpressure() {
    const PANIC_MESSAGE: &str = "parallel processor backpressure panic";
    let mut processor = ParallelBatchProcessor::builder(|item: &i32| {
        if *item == 0 {
            panic!("{PANIC_MESSAGE}");
        }
    })
    .thread_count(NonZeroUsize::new(1).expect("thread count is non-zero"))
    .sequential_threshold(0)
    .build();

    let payload = catch_unwind(AssertUnwindSafe(|| {
        processor.process_with_count((0..64).collect::<Vec<_>>(), 64)
    }))
    .expect_err("worker panic should be propagated without blocking the producer");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

/// Test item borrowing a stack-owned counter.
struct BorrowedItem<'a> {
    /// Counter incremented by the processor consumer.
    counter: &'a AtomicCount,
}
