// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Chunk storage reuse must preserve consumption and destruction boundaries.
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;

use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessError;
use qubit_batch::ChunkedBatchProcessor;

struct Item {
    index: usize,
    drops: Rc<RefCell<Vec<usize>>>,
}
impl Drop for Item {
    fn drop(&mut self) {
        self.drops.borrow_mut().push(self.index);
    }
}
#[derive(Clone, Copy)]
enum Behavior {
    Full,
    Reject,
    ClaimFull,
    ClaimPartial,
    Panic,
}
struct Delegate {
    behavior: Behavior,
    first_items: Vec<usize>,
}
impl BatchProcessor<Item> for Delegate {
    type Error = &'static str;
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let mut items = items.into_iter();
        let first = items.next().expect("nonempty chunks");
        self.first_items.push(first.index);
        drop(first);
        let completed = match self.behavior {
            Behavior::Full => 1 + items.count(),
            Behavior::Reject => return Err("rejected after one item"),
            Behavior::ClaimFull => count,
            Behavior::ClaimPartial => 1,
            Behavior::Panic => panic!("delegate panic"),
        };
        BatchProcessResult::builder(count)
            .completed_count(completed)
            .processed_count(completed)
            .chunk_count(1)
            .build()
            .map_err(|_| "invalid result")
    }
}
/// Builds owned items and an independent destruction log.
fn source() -> (Vec<Item>, Rc<RefCell<Vec<usize>>>) {
    let drops = Rc::new(RefCell::new(Vec::new()));
    let items = (0..5)
        .map(|index| Item {
            index,
            drops: Rc::clone(&drops),
        })
        .collect();
    (items, drops)
}
/// Verifies exactly-once destruction without promising destruction order.
fn assert_drops(drops: &Rc<RefCell<Vec<usize>>>) {
    let mut actual = drops.borrow().clone();
    actual.sort_unstable();
    assert_eq!(actual, vec![0, 1, 2, 3, 4]);
}
#[test]
fn test_full_and_claimed_chunks_keep_source_boundaries() {
    for behavior in [Behavior::Full, Behavior::ClaimFull] {
        let (items, drops) = source();
        let delegate = Delegate {
            behavior,
            first_items: Vec::new(),
        };
        let mut processor =
            ChunkedBatchProcessor::new(delegate, NonZeroUsize::new(2).expect("nonzero"));
        let result = processor.process(items).expect("valid delegate result");
        assert_eq!(result.completed_count(), 5);
        assert_eq!(result.chunk_count(), 3);
        assert_eq!(processor.delegate().first_items, vec![0, 2, 4]);
        assert_drops(&drops);
    }
}
#[test]
fn test_partial_delegate_error_drops_every_item_once() {
    let (items, drops) = source();
    let delegate = Delegate {
        behavior: Behavior::Reject,
        first_items: Vec::new(),
    };
    let mut processor =
        ChunkedBatchProcessor::new(delegate, NonZeroUsize::new(2).expect("nonzero"));
    let error = processor.process(items).expect_err("delegate rejection");
    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            result,
            source,
            ..
        } => {
            assert_eq!((chunk_index, start_index, chunk_len), (0, 0, 2));
            assert_eq!(source, "rejected after one item");
            assert_eq!(result.completed_count(), 0);
        }
        error => panic!("unexpected error: {error:?}"),
    }
    assert_drops(&drops);
}
#[test]
fn test_partial_success_is_rejected_without_aggregating_chunk() {
    let (items, drops) = source();
    let delegate = Delegate {
        behavior: Behavior::ClaimPartial,
        first_items: Vec::new(),
    };
    let mut processor =
        ChunkedBatchProcessor::new(delegate, NonZeroUsize::new(2).expect("nonzero"));
    match processor.process(items).expect_err("incomplete chunk") {
        ChunkedBatchProcessError::InvalidChunkResult {
            completed_count,
            result,
            ..
        } => {
            assert_eq!(completed_count, 1);
            assert_eq!(result.completed_count(), 0);
        }
        error => panic!("unexpected error: {error:?}"),
    }
    assert_drops(&drops);
}
#[test]
fn test_delegate_panic_drops_remaining_items() {
    let (items, drops) = source();
    let delegate = Delegate {
        behavior: Behavior::Panic,
        first_items: Vec::new(),
    };
    let mut processor =
        ChunkedBatchProcessor::new(delegate, NonZeroUsize::new(2).expect("nonzero"));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = processor.process(items);
        }))
        .is_err()
    );
    assert_drops(&drops);
}
