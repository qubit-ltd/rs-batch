/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shared test processor for chunked batch processing.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        Mutex,
    },
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestChunkOutcome {
    Success,
    Failure(&'static str),
    InvalidCompletedCount,
    InvalidItemCount,
}

#[derive(Debug, Default)]
pub struct TestChunkProcessor {
    chunks: Arc<Mutex<Vec<Vec<i32>>>>,
    outcomes: Arc<Mutex<VecDeque<TestChunkOutcome>>>,
}

impl TestChunkProcessor {
    pub fn success() -> Self {
        Self::default()
    }

    pub fn outcomes<I>(outcomes: I) -> Self
    where
        I: IntoIterator<Item = TestChunkOutcome>,
    {
        Self {
            chunks: Arc::default(),
            outcomes: Arc::new(Mutex::new(outcomes.into_iter().collect())),
        }
    }

    pub fn chunks(&self) -> Arc<Mutex<Vec<Vec<i32>>>> {
        Arc::clone(&self.chunks)
    }
}

impl BatchProcessor<i32> for TestChunkProcessor {
    type Error = &'static str;

    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let chunk = items.into_iter().collect::<Vec<_>>();
        assert_eq!(chunk.len(), count);
        self.chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(chunk);

        match self
            .outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(TestChunkOutcome::Success)
        {
            TestChunkOutcome::Success => Ok(valid_chunk_result(count)),
            TestChunkOutcome::Failure(message) => Err(message),
            TestChunkOutcome::InvalidCompletedCount => {
                let completed_count = count.saturating_sub(1);
                Ok(BatchProcessResult::builder(count)
                    .completed_count(completed_count)
                    .processed_count(completed_count)
                    .chunk_count(if completed_count == 0 { 0 } else { 1 })
                    .build()
                    .expect("invalid-completed test result counters should be valid"))
            }
            TestChunkOutcome::InvalidItemCount => Ok(BatchProcessResult::builder(count + 1)
                .completed_count(count)
                .processed_count(count)
                .chunk_count(if count == 0 { 0 } else { 1 })
                .build()
                .expect("invalid-item-count test result counters should be valid")),
        }
    }
}

fn valid_chunk_result(count: usize) -> BatchProcessResult {
    BatchProcessResult::builder(count)
        .completed_count(count)
        .processed_count(count)
        .chunk_count(if count == 0 { 0 } else { 1 })
        .build()
        .expect("test chunk result counters should be valid")
}
