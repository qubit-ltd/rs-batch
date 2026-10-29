// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchProcessor`](qubit_batch::ParallelBatchProcessor).

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use qubit_atomic::AtomicCount;
use qubit_batch::BatchProcessError;
use qubit_batch::BatchProcessor;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::ParallelBatchProcessor;
use qubit_batch::ParallelBatchProcessorBuildError;
use qubit_batch::ProgressFailure;
use qubit_function::Consumer;
use qubit_progress::AutoReporterError;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

use crate::support::FailingReporter;
use crate::support::PhaseRecordingReporter;
use crate::support::ProgressEvent;
use crate::support::RecordingReporter;
use crate::support::panic_payload_message;
use crate::support::run_with_watchdog;

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
        *accepted.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![5, 6]
    );
}

#[test]
fn test_parallel_batch_processor_accessors_and_value_reporter() {
    let processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .reporter(RecordingReporter::new())
        .sequential_threshold(7)
        .report_interval(Duration::from_millis(25))
        .build()
        .expect("parallel processor should build");
    let no_reporter_processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .no_reporter()
        .build()
        .expect("parallel processor should build");

    assert_eq!(processor.report_interval(), Duration::from_millis(25));
    assert_eq!(processor.sequential_threshold(), 7);
    assert!(Arc::strong_count(processor.reporter()) >= 1);
    assert_eq!(
        no_reporter_processor.report_interval(),
        ParallelBatchProcessor::<i32>::DEFAULT_REPORT_INTERVAL
    );
}

#[test]
fn test_parallel_batch_processor_rejects_zero_thread_count() {
    let result = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(0)
        .build();

    match result {
        Err(error) => {
            assert_eq!(error, ParallelBatchProcessorBuildError::ZeroThreadCount)
        }
        Ok(_) => panic!("zero worker count should be rejected"),
    }
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
fn test_parallel_batch_processor_terminal_phases_for_parallel_and_sequential_paths() {
    run_with_watchdog(
        concat!(
            module_path!(),
            "::test_parallel_batch_processor_terminal_phases_for_parallel_and_sequential_paths"
        ),
        || {
            for threshold in [0usize, 100] {
                let reporter = Arc::new(PhaseRecordingReporter::new());
                let concurrent_items = Arc::new(Barrier::new(2));
                let worker_barrier = Arc::clone(&concurrent_items);
                let mut processor = ParallelBatchProcessor::builder(move |_item: &i32| {
                    if threshold == 0 {
                        worker_barrier.wait();
                    }
                })
                .thread_count(2)
                .sequential_threshold(threshold)
                .reporter_arc(reporter.clone())
                .build()
                .expect("two-worker configuration should build");

                let result = processor
                    .process_with_count([1, 2], 2)
                    .expect("exact source should succeed");

                assert!(result.is_success());
                assert_eq!(result.item_count(), 2);
                assert_eq!(result.completed_count(), 2);
                assert_eq!(result.processed_count(), 2);
                let phases = reporter.phases();
                assert_eq!(phases.first(), Some(&Phase::Started));
                assert_eq!(phases.last(), Some(&Phase::Succeeded));
                assert_eq!(phases.iter().filter(|phase| **phase == Phase::Started).count(), 1);
                assert_eq!(
                    phases
                        .iter()
                        .filter(|phase| matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled))
                        .count(),
                    1
                );
            }
        },
    );
}

#[test]
fn test_parallel_batch_processor_reports_progress() {
    run_with_watchdog(
        concat!(module_path!(), "::test_parallel_batch_processor_reports_progress"),
        || {
            let reporter = Arc::new(CoordinatedRecordingReporter::new());
            let shared = Arc::clone(&reporter.released);
            let entered = Arc::new(Barrier::new(2));
            let mut processor = ParallelBatchProcessor::builder(move |item: &i32| {
                if *item < 2 {
                    entered.wait();
                }
                if *item == 1 {
                    let (released, ready) = &*shared;
                    let released = released.lock().expect("release lock should be healthy");
                    let _released = ready
                        .wait_while(released, |released| !*released)
                        .expect("release wait should be healthy");
                }
            })
            .thread_count(2)
            .sequential_threshold(0)
            .reporter_arc(reporter.clone())
            .report_interval(Duration::ZERO)
            .build()
            .expect("parallel processor should build");

            let result = processor
                .process_with_count(vec![0, 1, 2, 3], 4)
                .expect("parallel processing should succeed");
            let events = reporter.events();

            assert_eq!(result.completed_count(), 4);
            assert!(matches!(events.first(), Some(ProgressEvent::Start { total_count: 4 })));
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
        },
    );
}

#[test]
fn test_parallel_batch_processor_preserves_completed_count_when_terminal_report_fails() {
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(2)
        .sequential_threshold(0)
        .report_interval(Duration::from_secs(3600))
        .reporter(FailingReporter::after_successes(1))
        .build()
        .expect("parallel processor should build");

    let error = processor
        .process_with_count([1], 1)
        .expect_err("terminal progress reporting should fail");

    assert!(matches!(&error, BatchProcessError::ProgressReport { .. }));
    assert_eq!(error.result().completed_count(), 1);
    assert_eq!(error.result().processed_count(), 1);
}

#[test]
fn test_parallel_batch_processor_reports_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingReporter::new());
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(2)
        .sequential_threshold(0)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::ZERO)
        .build()
        .expect("parallel processor should build");

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
fn test_parallel_batch_processor_reports_sequential_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingReporter::new());
    let mut processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(2)
        .sequential_threshold(3)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::ZERO)
        .build()
        .expect("parallel processor should build");

    let result = processor
        .process_with_count(vec![1, 2, 3], 3)
        .expect("small batch should process through the sequential fallback");
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
    run_with_watchdog(
        concat!(
            module_path!(),
            "::test_parallel_batch_processor_uses_configured_thread_count"
        ),
        || {
            let active = Arc::new(AtomicUsize::new(0));
            let maximum = Arc::new(AtomicUsize::new(0));
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let work = {
                let active = Arc::clone(&active);
                let maximum = Arc::clone(&maximum);
                move |index: usize| {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    if index < 2 {
                        entered.wait();
                        maximum.fetch_max(active.load(Ordering::SeqCst), Ordering::SeqCst);
                        release.wait();
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                }
            };
            let mut processor = ParallelBatchProcessor::builder(move |index: &usize| work(*index))
                .thread_count(2)
                .sequential_threshold(0)
                .build()
                .expect("parallel processor should build");
            let result = processor
                .process_with_count(0usize..6, 6)
                .expect("parallel processing should succeed");

            assert_eq!(processor.thread_count(), 2);
            assert_eq!(processor.sequential_threshold(), 0);
            assert_eq!(result.completed_count(), 6);
            assert_eq!(result.processed_count(), 6);
            assert_eq!(maximum.load(Ordering::SeqCst), 2);
            assert_eq!(active.load(Ordering::SeqCst), 0);
        },
    );
}

#[test]
fn test_parallel_batch_processor_parallel_count_mismatches() {
    run_with_watchdog(
        concat!(
            module_path!(),
            "::test_parallel_batch_processor_parallel_count_mismatches"
        ),
        || {
            for threshold in [0usize, 100] {
                for (items, declared, expected_values, exceeded) in [
                    (vec![1, 2, 3], 2usize, vec![1, 2], true),
                    (vec![1, 2], 3usize, vec![1, 2], false),
                ] {
                    let seen = Arc::new(Mutex::new(Vec::new()));
                    let recorded = Arc::clone(&seen);
                    let reporter = Arc::new(PhaseRecordingReporter::new());
                    let concurrent_items = Arc::new(Barrier::new(2));
                    let worker_barrier = Arc::clone(&concurrent_items);
                    let callback_count = Arc::new(AtomicUsize::new(0));
                    let worker_callback_count = Arc::clone(&callback_count);
                    let mut processor = ParallelBatchProcessor::builder(move |item: &i32| {
                        recorded.lock().expect("value log should be healthy").push(*item);
                        let callback_index = worker_callback_count.fetch_add(1, Ordering::SeqCst);
                        if threshold == 0 && callback_index < 2 {
                            worker_barrier.wait();
                        }
                    })
                    .thread_count(2)
                    .sequential_threshold(threshold)
                    .reporter_arc(reporter.clone())
                    .build()
                    .expect("parallel processor should build");
                    let error = processor
                        .process_with_count(items, declared)
                        .expect_err("count should mismatch");
                    match &error {
                        BatchProcessError::CountExceeded {
                            expected,
                            observed_at_least,
                            ..
                        } if exceeded => {
                            assert_eq!((*expected, *observed_at_least), (2, 3));
                        }
                        BatchProcessError::CountShortfall { expected, actual, .. } if !exceeded => {
                            assert_eq!((*expected, *actual), (3, 2));
                        }
                        other => panic!("unexpected count error: {other:?}"),
                    }
                    assert_eq!(error.result().completed_count(), 2);
                    assert_eq!(error.result().processed_count(), 2);
                    assert_eq!(error.result().chunk_count(), 1);
                    let phases = reporter.phases();
                    assert_eq!(phases.first(), Some(&Phase::Started));
                    assert_eq!(phases.last(), Some(&Phase::Failed));
                    assert_eq!(phases.iter().filter(|phase| **phase == Phase::Started).count(), 1);
                    assert_eq!(
                        phases
                            .iter()
                            .filter(|phase| matches!(phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled))
                            .count(),
                        1
                    );
                    let mut values = seen.lock().expect("value log should be healthy").clone();
                    values.sort_unstable();
                    assert_eq!(values, expected_values);
                }
            }
        },
    );
}

#[test]
fn test_parallel_batch_processor_running_failures_stop_source() {
    run_with_watchdog(
        concat!(
            module_path!(),
            "::test_parallel_batch_processor_running_failures_stop_source"
        ),
        || {
            for panics in [false, true] {
                let released = Arc::new((Mutex::new(false), Condvar::new()));
                let completed = Arc::new(AtomicUsize::new(0));
                let observed = Arc::new(AtomicUsize::new(0));
                let worker_release = Arc::clone(&released);
                let worker_completed = Arc::clone(&completed);
                let mut processor = ParallelBatchProcessor::builder(move |item: &usize| {
                    if *item != 0 {
                        let (released, ready) = &*worker_release;
                        let released = released.lock().expect("release lock should be healthy");
                        let _released = ready
                            .wait_while(released, |released| !*released)
                            .expect("release wait should be healthy");
                    }
                    worker_completed.fetch_add(1, Ordering::SeqCst);
                })
                .thread_count(2)
                .sequential_threshold(0)
                .report_interval(Duration::ZERO)
                .reporter(RunningFailureReporter {
                    released: Arc::clone(&released),
                    panics,
                })
                .build()
                .expect("parallel processor should build");
                let pulls = Arc::clone(&observed);
                let source = (0usize..).inspect(move |_| {
                    pulls.fetch_add(1, Ordering::SeqCst);
                });
                let error = processor
                    .process_with_count(source, usize::MAX)
                    .expect_err("running reporter failure should stop the operation");
                let BatchProcessError::ProgressReport { source, result } = error else {
                    panic!("running failure should retain a progress error");
                };
                match (panics, source.as_ref()) {
                    (false, ProgressFailure::AutoReporter(AutoReporterError::Emission(_))) => {}
                    (true, ProgressFailure::AutoReporter(AutoReporterError::Panicked(panic))) => {
                        assert_eq!(panic.message(), Some("synthetic processor running panic"));
                    }
                    other => panic!("unexpected running failure: {other:?}"),
                }
                let completed = completed.load(Ordering::SeqCst);
                assert!(completed > 0);
                assert!(completed < usize::MAX);
                assert_eq!(result.item_count(), usize::MAX);
                assert_eq!(result.completed_count(), completed);
                assert_eq!(result.processed_count(), completed);
                assert!(observed.load(Ordering::SeqCst) >= completed);
            }
        },
    );
}

struct RunningFailureReporter {
    released: Arc<(Mutex<bool>, Condvar)>,
    panics: bool,
}

impl Reporter for RunningFailureReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if event.phase() == Phase::Running {
            let (released, ready) = &*self.released;
            *released.lock().expect("release lock should be healthy") = true;
            ready.notify_all();
            if self.panics {
                panic!("synthetic processor running panic");
            }
            return Err(ReporterError::message("synthetic processor running failure"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct CoordinatedRecordingReporter {
    events: Mutex<Vec<ProgressEvent>>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl CoordinatedRecordingReporter {
    fn new() -> Self {
        Self::default()
    }

    fn events(&self) -> Vec<ProgressEvent> {
        self.events.lock().expect("event log should be healthy").clone()
    }
}

impl Reporter for CoordinatedRecordingReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        let counter = event.metrics().first().expect("batch event should have one metric");
        let total_count = usize::try_from(counter.total().unwrap_or(counter.completed()))
            .expect("test progress count should fit usize");
        let recorded = match event.phase() {
            Phase::Started => ProgressEvent::Start { total_count },
            Phase::Running => ProgressEvent::Process {
                total_count,
                active_count: usize::try_from(counter.active()).expect("active count should fit usize"),
                completed_count: usize::try_from(counter.completed()).expect("completed count should fit usize"),
            },
            Phase::Succeeded | Phase::Failed | Phase::Cancelled => ProgressEvent::Finish {
                total_count,
                completed_count: usize::try_from(counter.completed()).expect("completed count should fit usize"),
            },
        };
        self.events.lock().expect("event log should be healthy").push(recorded);
        if event.phase() == Phase::Running && counter.active() > 0 {
            let (released, ready) = &*self.released;
            *released.lock().expect("release lock should be healthy") = true;
            ready.notify_all();
        }
        Ok(())
    }
}

#[test]
fn test_parallel_batch_processor_uses_sequential_threshold() {
    let caller_thread = thread::current().id();
    let observed_threads = Arc::new(Mutex::new(Vec::new()));
    let observed_by_consumer = Arc::clone(&observed_threads);
    let mut processor = ParallelBatchProcessor::builder(move |_item: &i32| {
        observed_by_consumer
            .lock()
            .expect("thread log should be healthy")
            .push(thread::current().id());
    })
    .thread_count(2)
    .build()
    .expect("parallel processor should build");

    let result = processor
        .process_with_count(vec![0, 1, 2, 3, 4, 5], 6)
        .expect("small batch should process through the sequential fallback");

    assert_eq!(
        processor.sequential_threshold(),
        ParallelBatchProcessor::<i32>::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(result.completed_count(), 6);
    assert_eq!(result.processed_count(), 6);
    assert_eq!(
        *observed_threads.lock().expect("thread log should be healthy"),
        vec![caller_thread; 6]
    );
}

#[test]
fn test_parallel_batch_processor_supports_non_static_items() {
    let first = AtomicCount::zero();
    let second = AtomicCount::zero();
    let mut processor = ParallelBatchProcessor::builder(|item: &BorrowedItem<'_>| {
        item.counter.inc();
    })
    .thread_count(2)
    .sequential_threshold(0)
    .build()
    .expect("parallel processor should build");
    let items = [BorrowedItem { counter: &first }, BorrowedItem { counter: &second }];

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
    .thread_count(2)
    .build()
    .expect("parallel processor should build");

    let error = processor
        .process_with_count(vec![1, 2, 3], 2)
        .expect_err("extra input should be reported");

    match error {
        BatchProcessError::CountExceeded {
            expected,
            observed_at_least,
            result,
            ..
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
            ..
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
        .thread_count(2)
        .build()
        .expect("parallel processor should build");

    let error = processor
        .process_with_count(vec![1, 2], 3)
        .expect_err("short input should be reported");

    match error {
        BatchProcessError::CountShortfall {
            expected,
            actual,
            result,
            ..
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
    .thread_count(2)
    .build()
    .expect("parallel processor should build");

    let payload = catch_unwind(AssertUnwindSafe(|| processor.process_with_count(vec![1], 1)))
        .expect_err("consumer panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_parallel_batch_processor_propagates_worker_panic_with_large_source() {
    run_with_watchdog(
        concat!(
            module_path!(),
            "::test_parallel_batch_processor_propagates_worker_panic_with_large_source"
        ),
        || {
            const PANIC_MESSAGE: &str = "parallel processor backpressure panic";
            let mut processor = ParallelBatchProcessor::builder(|item: &i32| {
                if *item == 0 {
                    panic!("{PANIC_MESSAGE}");
                }
            })
            .thread_count(2)
            .sequential_threshold(0)
            .build()
            .expect("parallel processor should build");

            let payload = catch_unwind(AssertUnwindSafe(|| {
                processor.process_with_count((0..64).collect::<Vec<_>>(), 64)
            }))
            .expect_err("worker panic should be propagated without blocking the producer");

            assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
        },
    );
}

/// Test item borrowing a stack-owned counter.
struct BorrowedItem<'a> {
    /// Counter incremented by the processor consumer.
    counter: &'a AtomicCount,
}

#[test]
fn test_parallel_batch_processor_preserves_items_on_scoped_workers() {
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let accepted_by_consumer = Arc::clone(&accepted);
    let mut processor = ParallelBatchProcessor::builder(move |item: &i32| {
        accepted_by_consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(*item);
    })
    .thread_count(2)
    .sequential_threshold(0)
    .build()
    .expect("parallel processor should build");

    let result = processor
        .process_with_count(vec![1, 2, 3, 4], 4)
        .expect("parallel processing should succeed");
    let mut accepted = accepted
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    accepted.sort_unstable();

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.processed_count(), 4);
    assert_eq!(accepted, vec![1, 2, 3, 4]);
}
