// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scenario tests for stateful bulk processors and chunk boundaries.

use std::num::NonZeroUsize;

use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessError;
use qubit_batch::ChunkedBatchProcessor;

#[derive(Default)]
struct Store {
    persisted: Vec<usize>,
    calls: Vec<usize>,
    affected_rows: usize,
}

struct BulkWriter<'a> {
    store: &'a mut Store,
    fail_call: Option<usize>,
}

impl BatchProcessor<usize> for BulkWriter<'_> {
    type Error = &'static str;

    fn process_with_count<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = usize>,
    {
        if count > 2 {
            return Err("driver input limit exceeded");
        }
        let items: Vec<_> = items.into_iter().collect();
        if items.len() != count {
            return Err("delegate count mismatch");
        }
        self.store.calls.push(count);
        if self.fail_call == Some(self.store.calls.len()) {
            if let Some(first) = items.first() {
                self.store.persisted.push(*first);
                self.store.affected_rows += 3;
            }
            return Err("remote failure after a partial side effect");
        }
        self.store.persisted.extend(items);
        self.store.affected_rows += count * 3;
        BatchProcessResult::builder(count)
            .completed_count(count)
            .processed_count(count)
            .chunk_count(usize::from(count > 0))
            .build()
            .map_err(|_| "invalid delegate counters")
    }
}

#[test]
fn test_bulk_chunks_respect_driver_limit_and_separate_domain_metrics() {
    let mut store = Store::default();
    let result = {
        let writer = BulkWriter {
            store: &mut store,
            fail_call: None,
        };
        let mut processor =
            ChunkedBatchProcessor::new(writer, NonZeroUsize::new(2).expect("chunk size should be nonzero"));
        processor.process(0..5).expect("all chunks should succeed")
    };
    assert_eq!(store.calls, [2, 2, 1]);
    assert_eq!(store.persisted, [0, 1, 2, 3, 4]);
    assert_eq!(store.affected_rows, 15);
    assert_eq!(result.processed_count(), 5);
    assert_eq!(result.completed_count(), 5);
    assert_eq!(result.chunk_count(), 3);
}

#[test]
fn test_failed_bulk_chunk_is_excluded_but_its_effects_are_not_rolled_back() {
    let mut store = Store::default();
    let error = {
        let writer = BulkWriter {
            store: &mut store,
            fail_call: Some(2),
        };
        let mut processor =
            ChunkedBatchProcessor::new(writer, NonZeroUsize::new(2).expect("chunk size should be nonzero"));
        processor.process(0..5).expect_err("the second chunk should fail")
    };
    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            result,
            ..
        } => {
            assert_eq!((chunk_index, start_index, chunk_len), (1, 2, 2));
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.processed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        error => panic!("unexpected error: {error:?}"),
    }
    assert_eq!(store.calls, [2, 2]);
    assert_eq!(store.persisted, [0, 1, 2]);
    assert_eq!(store.affected_rows, 9);
}
