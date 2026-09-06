// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for batch processor result and failure-boundary contracts.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessResultBuildError;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessor;

struct PartialDelegate {
    calls: usize,
    effects: Rc<Cell<usize>>,
}

impl BatchProcessor<usize> for PartialDelegate {
    type Error = &'static str;

    fn process_with_count<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = usize>,
    {
        self.calls += 1;
        if self.calls == 2 {
            if items.into_iter().next().is_some() {
                self.effects.set(self.effects.get() + 1);
            }
            return Err("one input already applied in failing chunk");
        }
        let processed_count = items.into_iter().count();
        self.effects.set(self.effects.get() + processed_count);
        BatchProcessResult::builder(count)
            .completed_count(processed_count)
            .processed_count(processed_count)
            .chunk_count(1)
            .build()
            .map_err(|_| "invalid process result")
    }
}

struct CountInputs;

impl BatchProcessor<usize> for CountInputs {
    type Error = BatchProcessResultBuildError;

    fn process_with_count<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = usize>,
    {
        let processed_count = items.into_iter().count();
        BatchProcessResult::builder(count)
            .completed_count(processed_count)
            .processed_count(processed_count)
            .chunk_count(usize::from(processed_count > 0))
            .build()
    }
}

#[test]
fn test_failed_chunk_effect_is_not_an_aggregate_retry_boundary() {
    let effects = Rc::new(Cell::new(0));
    let delegate = PartialDelegate {
        calls: 0,
        effects: Rc::clone(&effects),
    };
    let mut processor =
        ChunkedBatchProcessor::new(delegate, NonZeroUsize::new(2).expect("chunk size should be non-zero"));

    let error = processor
        .process([0, 1, 2, 3])
        .expect_err("the second chunk should fail after applying one input");

    assert_eq!(error.result().completed_count(), 2);
    assert_eq!(error.result().processed_count(), 2);
    assert_eq!(error.result().chunk_count(), 1);
    assert_eq!(effects.get(), 3);
}

#[test]
fn test_processed_count_tracks_successful_inputs_not_affected_rows() {
    let affected_rows = Cell::new(6);

    let error = BatchProcessResult::builder(2)
        .completed_count(2)
        .processed_count(affected_rows.get())
        .chunk_count(1)
        .build()
        .expect_err("affected rows can exceed the completed input count");
    assert_eq!(
        error,
        BatchProcessResultBuildError::ProcessedCountExceeded {
            completed_count: 2,
            processed_count: 6,
        }
    );

    let result = BatchProcessResult::builder(2)
        .completed_count(2)
        .processed_count(2)
        .chunk_count(1)
        .build()
        .expect("successful input counts should satisfy the result invariant");
    assert_eq!(result.processed_count(), 2);
    assert_eq!(affected_rows.get(), 6);
}

#[test]
fn test_nested_chunk_count_tracks_successful_outer_chunks() {
    let inner = ChunkedBatchProcessor::new(
        CountInputs,
        NonZeroUsize::new(2).expect("inner chunk size should be non-zero"),
    );
    let mut outer = ChunkedBatchProcessor::new(
        inner,
        NonZeroUsize::new(4).expect("outer chunk size should be non-zero"),
    );

    let result = outer
        .process(0..8)
        .expect("both nested chunking layers should process every input");

    assert_eq!(result.item_count(), 8);
    assert_eq!(result.completed_count(), 8);
    assert_eq!(result.processed_count(), 8);
    assert_eq!(result.chunk_count(), 2);
}
