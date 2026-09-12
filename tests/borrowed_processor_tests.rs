// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for borrowed consumers in sequential batch processors.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessor;
use qubit_batch::SequentialBatchProcessor;
use qubit_batch::SequentialBatchProcessorBuilder;
use qubit_function::BoxConsumer;
use qubit_function::Consumer;

#[test]
fn test_borrowed_consumer_and_legacy_types_coexist() {
    let prefix = String::from("ok");
    let mut borrowed = SequentialBatchProcessor::with_consumer(|item: &String| {
        assert!(item.starts_with(&prefix));
    });
    assert!(
        borrowed
            .process([String::from("okay")])
            .expect("borrowed consumer should process the first batch")
            .is_success()
    );
    assert!(
        borrowed
            .process([String::from("ok")])
            .expect("borrowed consumer should process the second batch")
            .is_success()
    );

    let mut legacy: SequentialBatchProcessor<i32> = SequentialBatchProcessor::new(|_: &i32| {});
    assert!(
        legacy
            .process([1])
            .expect("legacy processor should remain usable")
            .is_success()
    );
    let old_builder: SequentialBatchProcessorBuilder<i32> = SequentialBatchProcessorBuilder::new(|_: &i32| {});
    let processor = old_builder.build();
    let _: &BoxConsumer<i32> = processor.consumer();
    let _: BoxConsumer<i32> = processor.into_consumer();

    let mut configured = SequentialBatchProcessorBuilder::with_consumer(|item: &String| {
        assert!(item.starts_with(&prefix));
    })
    .report_interval(Duration::ZERO)
    .no_reporter()
    .build();
    assert!(
        configured
            .process([String::from("ok")])
            .expect("configured borrowed consumer should process the batch")
            .is_success()
    );
}

#[test]
fn test_borrowed_consumer_remains_compatible_with_chunking() {
    let prefix = String::from("item-");
    let borrowed = SequentialBatchProcessor::with_consumer(|item: &String| {
        assert!(item.starts_with(&prefix));
    });
    let chunk_size = NonZeroUsize::new(2).expect("chunk size should be non-zero");
    let mut processor = ChunkedBatchProcessor::new(borrowed, chunk_size);

    let result = processor
        .process([String::from("item-1"), String::from("item-2"), String::from("item-3")])
        .expect("chunked borrowed consumer should process all items");

    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.chunk_count(), 2);
}

#[test]
fn test_sequential_consumer_does_not_require_send() {
    let accepted_count = Rc::new(Cell::new(0));
    let accepted_count_by_consumer = Rc::clone(&accepted_count);
    let mut processor = SequentialBatchProcessor::with_consumer(move |_item: &i32| {
        accepted_count_by_consumer.set(accepted_count_by_consumer.get() + 1);
    });

    processor.consumer().accept(&0);
    let result = processor
        .process([1, 2])
        .expect("non-Send consumer should process sequentially");

    assert!(result.is_success());
    assert_eq!(accepted_count.get(), 3);
}
