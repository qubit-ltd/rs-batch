/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for chunked batch processing.

use std::{
    error::Error,
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessError,
    ChunkedBatchProcessor,
    NoOpProgressReporter,
};

use crate::support::{
    ProgressEvent,
    RecordingProgressReporter,
};

#[derive(Debug, Default)]
struct RecordingProcessor {
    chunks: Arc<Mutex<Vec<Vec<i32>>>>,
}

impl RecordingProcessor {
    fn chunks(&self) -> Arc<Mutex<Vec<Vec<i32>>>> {
        Arc::clone(&self.chunks)
    }
}

impl BatchProcessor<i32> for RecordingProcessor {
    type Error = &'static str;

    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let chunk = items.into_iter().collect::<Vec<_>>();
        assert_eq!(chunk.len(), count);
        self.chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(chunk);
        Ok(BatchProcessResult::new(
            count,
            count,
            count,
            1,
            Duration::ZERO,
        ))
    }
}

#[test]
fn test_chunked_batch_processor_accessors_and_delegate_mutation() {
    let delegate = RecordingProcessor::default();
    let chunks = delegate.chunks();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(4).expect("chunk size is non-zero"),
    )
    .with_reporter(NoOpProgressReporter)
    .with_report_interval(Duration::from_millis(10));

    assert_eq!(processor.chunk_size().get(), 4);
    assert_eq!(processor.report_interval(), Duration::from_millis(10));
    processor.reporter().start(0);
    assert!(Arc::ptr_eq(&processor.delegate().chunks(), &chunks));
    assert!(Arc::ptr_eq(&processor.delegate_mut().chunks(), &chunks));

    let delegate = processor.into_delegate();
    assert!(Arc::ptr_eq(&delegate.chunks(), &chunks));
}

#[test]
fn test_chunked_batch_processor_submits_items_in_chunks() {
    let delegate = RecordingProcessor::default();
    let chunks = delegate.chunks();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let result = processor
        .process([1, 2, 3, 4, 5], 5)
        .expect("chunked processing should succeed");

    assert_eq!(result.item_count(), 5);
    assert_eq!(result.completed_count(), 5);
    assert_eq!(result.processed_count(), 5);
    assert_eq!(result.chunk_count(), 3);
    assert_eq!(
        *chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![vec![1, 2], vec![3, 4], vec![5]]
    );
}

#[test]
fn test_chunked_batch_processor_accepts_empty_input() {
    let delegate = RecordingProcessor::default();
    let chunks = delegate.chunks();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let result = processor
        .process([], 0)
        .expect("empty batch should succeed");

    assert_eq!(result.item_count(), 0);
    assert_eq!(result.completed_count(), 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.chunk_count(), 0);
    assert!(
        chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

#[test]
fn test_chunked_batch_processor_reports_progress() {
    let delegate = RecordingProcessor::default();
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .with_reporter_arc(reporter.clone())
    .with_report_interval(Duration::ZERO);

    processor
        .process([1, 2, 3], 3)
        .expect("chunked processing should succeed");

    let events = reporter.events();
    assert!(matches!(events[0], ProgressEvent::Start { total_count: 3 }));
    assert!(
        events.iter().any(|event| matches!(
            event,
            ProgressEvent::Process {
                total_count: 3,
                active_count: 0,
                completed_count: 2,
                ..
            }
        )),
        "expected a progress event after the first completed chunk: {events:?}"
    );
    assert!(
        matches!(
            events.last(),
            Some(ProgressEvent::Finish { total_count: 3, .. })
        ),
        "expected a finish event: {events:?}"
    );
}

#[test]
fn test_chunked_batch_processor_skips_progress_before_interval() {
    let delegate = RecordingProcessor::default();
    let reporter = Arc::new(RecordingProgressReporter::new());
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .with_reporter_arc(reporter.clone())
    .with_report_interval(Duration::from_secs(3_600));

    processor
        .process([1, 2], 2)
        .expect("chunked processing should succeed");

    let events = reporter.events();
    assert_eq!(
        events.len(),
        2,
        "expected only start and finish: {events:?}"
    );
    assert!(matches!(events[0], ProgressEvent::Start { total_count: 2 }));
    assert!(
        matches!(events[1], ProgressEvent::Finish { total_count: 2, .. }),
        "expected a finish event without intermediate progress: {events:?}"
    );
}

#[test]
fn test_chunked_batch_processor_reports_count_exceeded() {
    let delegate = RecordingProcessor::default();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process([1, 2, 3], 2)
        .expect_err("extra input should be reported");

    match error {
        ChunkedBatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_chunked_batch_processor_reports_count_exceeded_before_first_chunk() {
    let delegate = RecordingProcessor::default();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process([1], 0)
        .expect_err("extra input should be reported before any chunk");

    match error {
        ChunkedBatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 0);
            assert_eq!(observed_at_least, 1);
            assert_eq!(result.completed_count(), 0);
            assert_eq!(result.chunk_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_chunked_batch_processor_reports_count_shortfall() {
    let delegate = RecordingProcessor::default();
    let mut processor = ChunkedBatchProcessor::new(
        delegate,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process([1, 2, 3], 5)
        .expect_err("short input should be reported");

    match error {
        ChunkedBatchProcessError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, 5);
            assert_eq!(actual, 3);
            assert_eq!(result.completed_count(), 3);
            assert_eq!(result.chunk_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_chunked_batch_process_error_helpers_and_display() {
    let result = BatchProcessResult::new(3, 1, 1, 1, Duration::from_millis(5));
    let shortfall = ChunkedBatchProcessError::<TestProcessorError>::CountShortfall {
        expected: 3,
        actual: 1,
        result: result.clone(),
    };
    let exceeded = ChunkedBatchProcessError::<TestProcessorError>::CountExceeded {
        expected: 3,
        observed_at_least: 4,
        result: result.clone(),
    };
    let failed = ChunkedBatchProcessError::ChunkFailed {
        chunk_index: 2,
        start_index: 4,
        chunk_len: 2,
        source: TestProcessorError("delegate failed"),
        result: result.clone(),
    };

    assert_eq!(shortfall.result(), &result);
    assert_eq!(shortfall.clone().into_result(), result);
    assert_eq!(
        shortfall.to_string(),
        "batch item count shortfall: expected 3, actual 1"
    );
    assert_eq!(exceeded.result(), &result);
    assert_eq!(exceeded.clone().into_result(), result);
    assert_eq!(
        exceeded.to_string(),
        "batch item count exceeded: expected 3, observed at least 4"
    );
    assert_eq!(failed.result(), &result);
    assert_eq!(
        failed.to_string(),
        "batch chunk 2 failed at item 4 with 2 items"
    );
    assert_eq!(
        failed
            .source()
            .expect("chunk failure should expose source")
            .to_string(),
        "delegate failed"
    );
    assert!(shortfall.source().is_none());
    assert!(exceeded.source().is_none());
    assert_eq!(failed.into_result(), result);
}

#[derive(Debug)]
struct FailingProcessor;

impl BatchProcessor<i32> for FailingProcessor {
    type Error = &'static str;

    fn process<I>(&mut self, _items: I, _count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        Err("insert failed")
    }
}

#[derive(Debug)]
struct FailingSecondChunkProcessor {
    calls: usize,
}

impl BatchProcessor<i32> for FailingSecondChunkProcessor {
    type Error = &'static str;

    fn process<I>(&mut self, _items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        if self.calls == 0 {
            self.calls += 1;
            Ok(BatchProcessResult::new(
                count,
                count,
                count,
                1,
                Duration::ZERO,
            ))
        } else {
            Err("partial insert failed")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestProcessorError(&'static str);

impl fmt::Display for TestProcessorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestProcessorError {}

#[test]
fn test_chunked_batch_processor_wraps_delegate_error() {
    let mut processor = ChunkedBatchProcessor::new(
        FailingProcessor,
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process([1, 2, 3], 3)
        .expect_err("delegate failure should be reported");

    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            source,
            result,
        } => {
            assert_eq!(chunk_index, 0);
            assert_eq!(start_index, 0);
            assert_eq!(chunk_len, 2);
            assert_eq!(source, "insert failed");
            assert_eq!(result.completed_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_chunked_batch_processor_wraps_partial_chunk_error() {
    let mut processor = ChunkedBatchProcessor::new(
        FailingSecondChunkProcessor { calls: 0 },
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process([1, 2, 3], 3)
        .expect_err("partial chunk failure should be reported");

    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            source,
            result,
        } => {
            assert_eq!(chunk_index, 1);
            assert_eq!(start_index, 2);
            assert_eq!(chunk_len, 1);
            assert_eq!(source, "partial insert failed");
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
