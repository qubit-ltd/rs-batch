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

use std::sync::{Arc, Mutex};

use qubit_function::Consumer;

use qubit_batch::{BatchProcessError, BatchProcessor, SequentialBatchProcessor};

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
        .process(vec![1, 2, 3], 3)
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
fn test_sequential_batch_processor_accepts_empty_input() {
    let mut processor = SequentialBatchProcessor::new(|_item: &i32| {
        panic!("empty input should not call the consumer");
    });

    let result = processor
        .process(Vec::<i32>::new(), 0)
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
        .process(vec![1, 2, 3], 2)
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
        .process(vec![1, 2], 3)
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
