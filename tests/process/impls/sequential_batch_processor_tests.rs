/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`SequentialBatchProcessor`](qubit_batch::SequentialBatchProcessor).

use std::sync::{
    Arc,
    Mutex,
};
use std::time::Duration;

use qubit_function::Consumer;

use qubit_batch::{
    BatchProcessError,
    BatchProcessor,
    SequentialBatchProcessor,
};

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
};

#[test]
fn test_sequential_batch_processor_consumer_accessors() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let processor = SequentialBatchProcessor::new(move |item: &i32| {
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
fn test_sequential_batch_processor_accessors_and_value_reporter() {
    let processor = SequentialBatchProcessor::new(|_item: &i32| {})
        .with_reporter(RecordingProgressReporter::new())
        .with_report_interval(Duration::from_millis(25));

    assert_eq!(processor.report_interval(), Duration::from_millis(25));
    assert!(Arc::strong_count(processor.reporter()) >= 1);
}

#[test]
fn test_sequential_batch_processor_processes_items_in_order() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = SequentialBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    });

    let result = processor
        .process_with_count(vec![1, 2, 3], 3)
        .expect("sequential processing should succeed");

    assert_eq!(result.item_count(), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.processed_count(), 3);
    assert_eq!(result.chunk_count(), 1);
    assert_eq!(
        *accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![1, 2, 3]
    );
}

#[test]
fn test_sequential_batch_processor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = SequentialBatchProcessor::new(|_item: &i32| {
        std::thread::sleep(Duration::from_millis(2));
    })
    .with_reporter_arc(reporter.clone())
    .with_report_interval(Duration::from_millis(1));

    let result = processor
        .process_with_count(vec![1, 2, 3], 3)
        .expect("sequential processing should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 3);
    assert!(matches!(
        events.first(),
        Some(ProgressEvent::Start { total_count: 3 })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 3,
            completed_count,
            ..
        } if *completed_count >= 1
    )));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 3, .. })
    ));
}

#[test]
fn test_sequential_batch_processor_reports_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = SequentialBatchProcessor::new(|_item: &i32| {})
        .with_reporter_arc(reporter.clone())
        .with_report_interval(Duration::ZERO);

    let result = processor
        .process_with_count(vec![1, 2], 2)
        .expect("sequential processing should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 2);
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 2,
            completed_count,
            ..
        } if *completed_count >= 1
    )));
}

#[test]
fn test_sequential_batch_processor_accepts_empty_input() {
    let mut processor = SequentialBatchProcessor::new(|_item: &i32| {
        panic!("empty input should not call the consumer");
    });

    let result = processor
        .process_with_count(Vec::<i32>::new(), 0)
        .expect("empty sequential processing should succeed");

    assert_eq!(result.item_count(), 0);
    assert_eq!(result.completed_count(), 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.chunk_count(), 0);
}

#[test]
fn test_sequential_batch_processor_reports_count_exceeded() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = SequentialBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    });

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
    assert_eq!(
        *accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![1, 2]
    );
}

#[test]
fn test_sequential_batch_processor_reports_count_shortfall() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = SequentialBatchProcessor::new(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    });

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
    assert_eq!(
        *accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![1, 2]
    );
}
