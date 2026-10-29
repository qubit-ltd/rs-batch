// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for chunked batch processing.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::BatchProcessResult;
use qubit_batch::BatchProcessor;
use qubit_batch::ChunkedBatchProcessError;
use qubit_batch::ChunkedBatchProcessor;
use qubit_progress::Phase;
use qubit_progress::reporter::NoopReporter;

use crate::support::FailingReporter;
use crate::support::PhaseRecordingReporter;
use crate::support::ProgressEvent;
use crate::support::RecordingReporter;
use crate::support::TestChunkOutcome;
use crate::support::TestChunkProcessor;

fn assert_phase_lifecycle(reporter: &PhaseRecordingReporter, terminal: Phase) {
    let phases = reporter.phases();
    assert_eq!(phases.first(), Some(&Phase::Started));
    assert_eq!(phases.last(), Some(&terminal));
    assert_eq!(phases.iter().filter(|phase| **phase == Phase::Started).count(), 1);
    assert_eq!(
        phases
            .iter()
            .filter(|phase| matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled))
            .count(),
        1
    );
}

#[derive(Default)]
struct PartiallyProcessingDelegate {
    chunks: Vec<Vec<i32>>,
}

impl BatchProcessor<i32> for PartiallyProcessingDelegate {
    type Error = &'static str;

    fn process_with_count<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let chunk: Vec<_> = items.into_iter().collect();
        assert_eq!(chunk.len(), count);
        let processed = chunk.iter().filter(|item| **item % 2 == 0).count();
        self.chunks.push(chunk);
        BatchProcessResult::builder(count)
            .completed_count(count)
            .processed_count(processed)
            .chunk_count(usize::from(count != 0))
            .build()
            .map_err(|_| "invalid delegate counters")
    }
}

#[test]
fn test_chunked_batch_processor_returns_progress_report_error() {
    let mut processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::success(),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .reporter(FailingReporter::after_successes(1))
    .build();

    let error = processor
        .process_with_count([1], 1)
        .expect_err("failing reporter should fail chunked processing");

    assert!(matches!(&error, ChunkedBatchProcessError::ProgressReport { .. }));
    assert_eq!(error.result().completed_count(), 1);
}

#[test]
fn test_chunked_batch_processor_accessors_and_delegate_mutation() {
    let delegate = TestChunkProcessor::success();
    let chunks = delegate.chunks();
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(4).expect("chunk size is non-zero"))
        .reporter(NoopReporter)
        .report_interval(Duration::from_millis(10))
        .build();

    assert_eq!(processor.chunk_size().get(), 4);
    assert_eq!(processor.report_interval(), Duration::from_millis(10));
    assert!(Arc::strong_count(processor.reporter()) >= 1);
    assert!(Arc::ptr_eq(&processor.delegate().chunks(), &chunks));
    assert!(Arc::ptr_eq(&processor.delegate_mut().chunks(), &chunks));

    let delegate = processor.into_delegate();
    assert!(Arc::ptr_eq(&delegate.chunks(), &chunks));

    let no_reporter_processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::success(),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .no_reporter()
    .build();
    assert!(Arc::strong_count(no_reporter_processor.reporter()) >= 1);
}

#[test]
fn test_chunked_batch_processor_submits_items_in_chunks() {
    let delegate = TestChunkProcessor::success();
    let chunks = delegate.chunks();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let result = processor
        .process_with_count([1, 2, 3, 4, 5], 5)
        .expect("chunked processing should succeed");

    assert_eq!(result.item_count(), 5);
    assert_eq!(result.completed_count(), 5);
    assert_eq!(result.processed_count(), 5);
    assert_eq!(result.chunk_count(), 3);
    assert_eq!(
        *chunks.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![vec![1, 2], vec![3, 4], vec![5]]
    );
    assert_phase_lifecycle(&reporter, Phase::Succeeded);
}

#[test]
fn test_chunked_batch_processor_aggregates_partial_processed_counts() {
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(
        PartiallyProcessingDelegate::default(),
        NonZeroUsize::new(2).expect("chunk size must be positive"),
    )
    .reporter_arc(reporter.clone())
    .build();

    let result = processor
        .process([1, 2, 3, 4])
        .expect("partial processed counts are valid completed chunks");

    assert_eq!(result.item_count(), 4);
    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(result.chunk_count(), 2);
    assert!(!result.is_success());
    assert_eq!(processor.delegate().chunks, vec![vec![1, 2], vec![3, 4]]);

    assert_phase_lifecycle(&reporter, Phase::Succeeded);
}

#[test]
fn test_chunked_batch_processor_accepts_empty_input() {
    let delegate = TestChunkProcessor::success();
    let chunks = delegate.chunks();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let result = processor.process_with_count([], 0).expect("empty batch should succeed");

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
    assert_phase_lifecycle(&reporter, Phase::Succeeded);
}

#[test]
fn test_chunked_batch_processor_reports_progress() {
    let delegate = TestChunkProcessor::success();
    let reporter = Arc::new(RecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .report_interval(Duration::ZERO)
        .build();

    let _ = processor
        .process_with_count([1, 2, 3], 3)
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
        matches!(events.last(), Some(ProgressEvent::Finish { total_count: 3, .. })),
        "expected a finish event: {events:?}"
    );
}

#[test]
fn test_chunked_batch_processor_skips_progress_before_interval() {
    let delegate = TestChunkProcessor::success();
    let reporter = Arc::new(RecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .report_interval(Duration::from_secs(3_600))
        .build();

    let _ = processor
        .process_with_count([1, 2], 2)
        .expect("chunked processing should succeed");

    let events = reporter.events();
    assert_eq!(events.len(), 2, "expected only start and finish: {events:?}");
    assert!(matches!(events[0], ProgressEvent::Start { total_count: 2 }));
    assert!(
        matches!(events[1], ProgressEvent::Finish { total_count: 2, .. }),
        "expected a finish event without intermediate progress: {events:?}"
    );
}

#[test]
fn test_chunked_batch_processor_reports_count_exceeded() {
    let delegate = TestChunkProcessor::success();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let error = processor
        .process_with_count([1, 2, 3], 2)
        .expect_err("extra input should be reported");

    match error {
        ChunkedBatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
            ..
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_flushes_tail_chunk_before_count_exceeded() {
    let delegate = TestChunkProcessor::success();
    let chunks = delegate.chunks();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let error = processor
        .process_with_count([1, 2, 3, 4], 3)
        .expect_err("extra input should be reported after flushing declared tail");

    match error {
        ChunkedBatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
            ..
        } => {
            assert_eq!(expected, 3);
            assert_eq!(observed_at_least, 4);
            assert_eq!(result.completed_count(), 3);
            assert_eq!(result.processed_count(), 3);
            assert_eq!(result.chunk_count(), 2);
            assert_eq!(
                *chunks.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
                vec![vec![1, 2], vec![3]]
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_propagates_tail_chunk_error_before_count_exceeded() {
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::outcomes([
            TestChunkOutcome::Success,
            TestChunkOutcome::Failure("tail insert failed"),
        ]),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .reporter_arc(reporter.clone())
    .build();

    let error = processor
        .process_with_count([1, 2, 3, 4], 3)
        .expect_err("tail chunk failure should be reported before count overflow");

    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            source,
            result,
            ..
        } => {
            assert_eq!(chunk_index, 1);
            assert_eq!(start_index, 2);
            assert_eq!(chunk_len, 1);
            assert_eq!(source, "tail insert failed");
            assert_eq!(result.completed_count(), 2);
            assert_eq!(result.chunk_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_reports_count_exceeded_before_first_chunk() {
    let delegate = TestChunkProcessor::success();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let error = processor
        .process_with_count([1], 0)
        .expect_err("extra input should be reported before any chunk");

    match error {
        ChunkedBatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
            ..
        } => {
            assert_eq!(expected, 0);
            assert_eq!(observed_at_least, 1);
            assert_eq!(result.completed_count(), 0);
            assert_eq!(result.chunk_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_reports_count_shortfall() {
    let delegate = TestChunkProcessor::success();
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(delegate, NonZeroUsize::new(2).expect("chunk size is non-zero"))
        .reporter_arc(reporter.clone())
        .build();

    let error = processor
        .process_with_count([1, 2, 3], 5)
        .expect_err("short input should be reported");

    match error {
        ChunkedBatchProcessError::CountShortfall {
            expected,
            actual,
            result,
            ..
        } => {
            assert_eq!(expected, 5);
            assert_eq!(actual, 3);
            assert_eq!(result.completed_count(), 3);
            assert_eq!(result.chunk_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_process_error_helpers_and_display() {
    let result = BatchProcessResult::builder(3)
        .completed_count(1)
        .processed_count(1)
        .chunk_count(1)
        .elapsed(Duration::from_millis(5))
        .build()
        .expect("process result counters should be valid");
    let shortfall = ChunkedBatchProcessError::<TestProcessorError>::CountShortfall {
        expected: 3,
        actual: 1,
        result: result.clone(),
        report_error: None,
    };
    let exceeded = ChunkedBatchProcessError::<TestProcessorError>::CountExceeded {
        expected: 3,
        observed_at_least: 4,
        result: result.clone(),
        report_error: None,
    };
    let failed = ChunkedBatchProcessError::ChunkFailed {
        chunk_index: 2,
        start_index: 4,
        chunk_len: 2,
        source: TestProcessorError("delegate failed"),
        result: result.clone(),
        report_error: None,
    };
    let invalid = ChunkedBatchProcessError::<TestProcessorError>::InvalidChunkResult {
        chunk_index: 1,
        start_index: 2,
        chunk_len: 2,
        item_count: 2,
        completed_count: 1,
        result: result.clone(),
        report_error: None,
    };

    assert_eq!(shortfall.result(), &result);
    assert_eq!(
        shortfall.to_string(),
        "batch item count shortfall: expected 3, actual 1"
    );
    assert_eq!(exceeded.result(), &result);
    assert_eq!(
        exceeded.to_string(),
        "batch item count exceeded: expected 3, observed at least 4"
    );
    assert_eq!(failed.result(), &result);
    assert_eq!(failed.to_string(), "batch chunk 2 failed at item 4 with 2 items");
    assert_eq!(
        failed.source().expect("chunk failure should expose source").to_string(),
        "delegate failed"
    );
    assert!(shortfall.source().is_none());
    assert!(exceeded.source().is_none());
    assert_eq!(invalid.result(), &result);
    assert_eq!(
        invalid.to_string(),
        concat!(
            "batch chunk 1 returned invalid result at item 2: expected 2 completed items, ",
            "got item_count 2, completed_count 1"
        )
    );
    assert!(invalid.source().is_none());
    assert_eq!(shortfall.into_result(), result.clone());
    assert_eq!(exceeded.into_result(), result.clone());
    assert_eq!(invalid.into_result(), result.clone());
    assert_eq!(failed.into_result(), result);
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
        TestChunkProcessor::outcomes([TestChunkOutcome::Failure("insert failed")]),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    );

    let error = processor
        .process_with_count([1, 2, 3], 3)
        .expect_err("delegate failure should be reported");

    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            source,
            result,
            ..
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
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::outcomes([
            TestChunkOutcome::Success,
            TestChunkOutcome::Failure("partial insert failed"),
        ]),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .reporter_arc(reporter.clone())
    .build();

    let error = processor
        .process_with_count([1, 2, 3], 3)
        .expect_err("partial chunk failure should be reported");

    match error {
        ChunkedBatchProcessError::ChunkFailed {
            chunk_index,
            start_index,
            chunk_len,
            source,
            result,
            ..
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
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_rejects_invalid_delegate_result() {
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::outcomes([TestChunkOutcome::InvalidCompletedCount]),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .reporter_arc(reporter.clone())
    .build();

    let error = processor
        .process_with_count([1, 2], 2)
        .expect_err("delegate result should describe the submitted chunk");

    match error {
        ChunkedBatchProcessError::InvalidChunkResult {
            chunk_index,
            start_index,
            chunk_len,
            item_count,
            completed_count,
            result,
            ..
        } => {
            assert_eq!(chunk_index, 0);
            assert_eq!(start_index, 0);
            assert_eq!(chunk_len, 2);
            assert_eq!(item_count, 2);
            assert_eq!(completed_count, 1);
            assert_eq!(result.completed_count(), 0);
            assert_eq!(result.processed_count(), 0);
            assert_eq!(result.chunk_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}

#[test]
fn test_chunked_batch_processor_rejects_invalid_delegate_item_count() {
    let reporter = Arc::new(PhaseRecordingReporter::new());
    let mut processor = ChunkedBatchProcessor::builder(
        TestChunkProcessor::outcomes([TestChunkOutcome::InvalidItemCount]),
        NonZeroUsize::new(2).expect("chunk size is non-zero"),
    )
    .reporter_arc(reporter.clone())
    .build();

    let error = processor
        .process_with_count([1, 2], 2)
        .expect_err("delegate result should describe the submitted chunk");

    match error {
        ChunkedBatchProcessError::InvalidChunkResult {
            chunk_index,
            start_index,
            chunk_len,
            item_count,
            completed_count,
            result,
            ..
        } => {
            assert_eq!(chunk_index, 0);
            assert_eq!(start_index, 0);
            assert_eq!(chunk_len, 2);
            assert_eq!(item_count, 3);
            assert_eq!(completed_count, 2);
            assert_eq!(result.completed_count(), 0);
            assert_eq!(result.processed_count(), 0);
            assert_eq!(result.chunk_count(), 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_phase_lifecycle(&reporter, Phase::Failed);
}
