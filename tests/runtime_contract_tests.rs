// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime concurrency contracts for the standard-thread batch executor.

use std::io;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_function::Runnable;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
use qubit_progress::reporter::NoopReporter;

const CHILD_PROCESS_ENV: &str = "QUBIT_BATCH_RUNTIME_CONTRACT_CHILD";
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a potentially blocking concurrency scenario in a killable child.
fn run_with_watchdog(test_name: &str, scenario: impl FnOnce()) {
    if std::env::var(CHILD_PROCESS_ENV).ok().as_deref() == Some(test_name) {
        scenario();
        return;
    }

    let mut child = Command::new(std::env::current_exe().expect("test executable should be available"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_PROCESS_ENV, test_name)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("runtime contract child should start");
    let deadline = Instant::now() + WATCHDOG_TIMEOUT;
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("runtime contract child status should be readable")
        {
            assert!(status.success(), "runtime contract child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("timed-out runtime contract child should be killable");
            let _ = child.wait();
            panic!("runtime contract child exceeded {WATCHDOG_TIMEOUT:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Iterator that reports each source item as the executor requests it.
struct ObservedItems {
    next: usize,
    count: usize,
    observed: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    max_unfinished: usize,
    window_full_sender: Option<mpsc::SyncSender<()>>,
}

impl ObservedItems {
    /// Creates an observed source that enforces the unfinished-item bound.
    fn new(
        count: usize,
        observed: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        max_unfinished: usize,
        window_full_sender: mpsc::SyncSender<()>,
    ) -> Self {
        Self {
            next: 0,
            count,
            observed,
            completed,
            max_unfinished,
            window_full_sender: Some(window_full_sender),
        }
    }
}

impl Iterator for ObservedItems {
    type Item = usize;

    /// Returns the next item after publishing its one-based observation count.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.count {
            return None;
        }
        let item = self.next;
        self.next += 1;
        let observed = self.observed.fetch_add(1, Ordering::AcqRel) + 1;
        let completed = self.completed.load(Ordering::Acquire);
        assert!(
            observed - completed <= self.max_unfinished,
            "unfinished source observations exceeded {}: observed {observed}, completed {completed}",
            self.max_unfinished,
        );
        if observed - completed == self.max_unfinished
            && let Some(sender) = self.window_full_sender.take()
        {
            sender.send(()).expect("window observer should remain alive");
        }
        Some(item)
    }

    /// Returns the exact remaining source length.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ObservedItems {}

/// Runnable that records completion in a shared counter.
struct CountingTask {
    completed: Arc<AtomicUsize>,
}

impl CountingTask {
    /// Creates a task that increments `completed` when run.
    fn new(completed: Arc<AtomicUsize>) -> Self {
        Self { completed }
    }
}

impl Runnable<()> for CountingTask {
    /// Records one successful task completion.
    fn run(&mut self) -> Result<(), ()> {
        self.completed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

/// Reporter whose first running event is released by the scheduler and fails.
struct CoordinatedFailingReporter {
    running_sender: mpsc::SyncSender<()>,
    failure_release: Mutex<mpsc::Receiver<()>>,
}

impl CoordinatedFailingReporter {
    /// Creates a reporter controlled by the supplied rendezvous channels.
    fn new(running_sender: mpsc::SyncSender<()>, failure_release: mpsc::Receiver<()>) -> Self {
        Self {
            running_sender,
            failure_release: Mutex::new(failure_release),
        }
    }
}

impl Reporter for CoordinatedFailingReporter {
    /// Fails the running event after the scheduler releases its rendezvous.
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if event.phase() != Phase::Running {
            return Ok(());
        }
        self.running_sender
            .send(())
            .expect("running-event receiver should remain alive");
        self.failure_release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv()
            .expect("scheduler should release the running failure");
        Err(ReporterError::new(io::Error::other(
            "synthetic running progress failure",
        )))
    }
}

#[test]
fn test_parallel_executor_keeps_concurrent_batch_outcomes_isolated() {
    run_with_watchdog(
        "test_parallel_executor_keeps_concurrent_batch_outcomes_isolated",
        || {
            let executor = ParallelBatchExecutor::builder()
                .thread_count(2)
                .sequential_threshold(0)
                .build()
                .expect("parallel executor should build");
            let first_completed = AtomicUsize::new(0);
            let second_completed = AtomicUsize::new(0);

            thread::scope(|scope| {
                let first = scope.spawn(|| {
                    executor
                        .for_each((0..64).map(|item| (0, item)), |(batch_id, _)| {
                            assert_eq!(batch_id, 0);
                            first_completed.fetch_add(1, Ordering::AcqRel);
                            Ok::<(), ()>(())
                        })
                        .expect("first batch should complete")
                });
                let second = scope.spawn(|| {
                    executor
                        .for_each((0..64).map(|item| (1, item)), |(batch_id, _)| {
                            assert_eq!(batch_id, 1);
                            second_completed.fetch_add(1, Ordering::AcqRel);
                            Ok::<(), ()>(())
                        })
                        .expect("second batch should complete")
                });
                let first_outcome = first.join().expect("first producer should join");
                let second_outcome = second.join().expect("second producer should join");

                assert_eq!(first_outcome.task_count(), 64);
                assert_eq!(first_outcome.completed_count(), 64);
                assert!(first_outcome.is_success());
                assert_eq!(second_outcome.task_count(), 64);
                assert_eq!(second_outcome.completed_count(), 64);
                assert!(second_outcome.is_success());
            });
            assert_eq!(first_completed.load(Ordering::Acquire), 64);
            assert_eq!(second_completed.load(Ordering::Acquire), 64);
        },
    );
}

#[test]
fn test_parallel_executor_bounds_unfinished_admission_window() {
    run_with_watchdog("test_parallel_executor_bounds_unfinished_admission_window", || {
        const ITEM_COUNT: usize = 64;
        const WORKER_COUNT: usize = 2;
        const MAX_UNFINISHED_ADMISSIONS: usize = 2 * WORKER_COUNT + 1;

        let executor = ParallelBatchExecutor::builder()
            .thread_count(WORKER_COUNT)
            .sequential_threshold(0)
            .build()
            .expect("parallel executor should build");
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let observed = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (window_full_sender, window_full_receiver) = mpsc::sync_channel(0);
        let (started_sender, started_receiver) = mpsc::sync_channel(ITEM_COUNT);

        thread::scope(|scope| {
            let runner = scope.spawn(|| {
                executor
                    .for_each_with_count(
                        ObservedItems::new(
                            ITEM_COUNT,
                            Arc::clone(&observed),
                            Arc::clone(&completed),
                            MAX_UNFINISHED_ADMISSIONS,
                            window_full_sender,
                        ),
                        ITEM_COUNT,
                        |_| {
                            started_sender
                                .send(())
                                .expect("task-start receiver should remain alive");
                            let (lock, ready) = gate.as_ref();
                            let mut released = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            while !*released {
                                released = ready.wait(released).unwrap_or_else(std::sync::PoisonError::into_inner);
                            }
                            completed.fetch_add(1, Ordering::AcqRel);
                            Ok::<(), ()>(())
                        },
                    )
                    .expect("bounded-window batch should complete")
            });

            for _ in 0..WORKER_COUNT {
                started_receiver
                    .recv()
                    .expect("both workers should start before release");
            }
            window_full_receiver
                .recv()
                .expect("blocked workers should fill the bounded admission window");
            assert_eq!(observed.load(Ordering::Acquire), MAX_UNFINISHED_ADMISSIONS);
            assert_eq!(completed.load(Ordering::Acquire), 0);

            let (lock, ready) = gate.as_ref();
            *lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            ready.notify_all();

            let outcome = runner.join().expect("bounded-window runner should join");
            assert_eq!(outcome.completed_count(), ITEM_COUNT);
        });
        assert_eq!(observed.load(Ordering::Acquire), ITEM_COUNT);
        assert_eq!(completed.load(Ordering::Acquire), ITEM_COUNT);
    });
}

#[test]
fn test_accepted_tokens_finish_after_failure_stops_admission() {
    let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO);
    let mut later_calls = 0;
    let outcome = coordinator
        .execute(
            std::iter::empty::<()>(),
            3,
            TaskFailurePolicy::StopOnFirstFailure,
            |_, context| {
                let first = context
                    .accept_task(|| Err::<(), ()>(()))
                    .expect("first task should be accepted");
                let second = context
                    .accept_task(|| {
                        later_calls += 1;
                        Ok::<(), ()>(())
                    })
                    .expect("second task should be accepted");
                context.execute_task(first);
                assert!(context.accept_task(|| Ok::<(), ()>(())).is_none());
                context.execute_task(second);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .expect("task failure policy should return an outcome");

    assert_eq!(later_calls, 1);
    assert_eq!(outcome.completed_count(), 2);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
}

#[test]
fn test_running_reporter_failure_stops_admission_and_drains_accepted_tokens() {
    run_with_watchdog(
        "test_running_reporter_failure_stops_admission_and_drains_accepted_tokens",
        || {
            // The probe must observe reporter-driven rejection long before a
            // count-exhaustion rejection can produce the same `None` result.
            const DECLARED_COUNT: usize = 1_000_000;

            let (running_sender, running_receiver) = mpsc::sync_channel(0);
            let (failure_release_sender, failure_release_receiver) = mpsc::sync_channel(0);
            let reporter = CoordinatedFailingReporter::new(running_sender, failure_release_receiver);
            let coordinator = ParallelBatchExecutionCoordinator::new(Arc::new(reporter), Duration::ZERO);
            let completed = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::new(AtomicUsize::new(0));

            let error = coordinator
                .execute(
                    std::iter::empty::<()>(),
                    DECLARED_COUNT,
                    TaskFailurePolicy::Continue,
                    |_, context| {
                        let first = context
                            .accept_task(CountingTask::new(Arc::clone(&completed)))
                            .expect("first task should be accepted");
                        let second = context
                            .accept_task(CountingTask::new(Arc::clone(&completed)))
                            .expect("second task should be accepted");
                        accepted.store(2, Ordering::Release);
                        let (worker_release_sender, worker_release_receiver) = mpsc::sync_channel(0);
                        let (worker_done_sender, worker_done_receiver) = mpsc::sync_channel(0);
                        let (admission_stopped_sender, admission_stopped_receiver) = mpsc::sync_channel(0);

                        thread::scope(|scope| {
                            let worker = scope.spawn(move || {
                                worker_release_receiver
                                    .recv()
                                    .expect("scheduler should release accepted worker");
                                context.execute_task(second);
                                worker_done_sender
                                    .send(())
                                    .expect("scheduler should observe worker completion");
                            });

                            context.execute_task(first);
                            running_receiver
                                .recv()
                                .expect("first completion should trigger a running event");
                            failure_release_sender
                                .send(())
                                .expect("running reporter should await failure release");

                            let admission_probe = scope.spawn(|| {
                                let mut probe_accepted = 0;
                                loop {
                                    let task = CountingTask::new(Arc::clone(&completed));
                                    let Some(task) = context.accept_task(task) else {
                                        admission_stopped_sender
                                            .send(probe_accepted)
                                            .expect("scheduler should await the admission-stop acknowledgement");
                                        return;
                                    };
                                    accepted.fetch_add(1, Ordering::AcqRel);
                                    probe_accepted += 1;
                                    context.execute_task(task);
                                }
                            });
                            let probe_accepted = admission_stopped_receiver
                                .recv()
                                .expect("admission probe should observe published reporter failure");
                            assert_eq!(accepted.load(Ordering::Acquire), probe_accepted + 2);
                            assert!(
                                accepted.load(Ordering::Acquire) < DECLARED_COUNT,
                                "admission-stop acknowledgement must precede count exhaustion",
                            );

                            worker_release_sender
                                .send(())
                                .expect("accepted worker should remain alive");
                            worker_done_receiver
                                .recv()
                                .expect("accepted worker should finish after reporter failure");
                            worker.join().expect("accepted worker should join");
                            admission_probe.join().expect("admission probe should join");
                        });
                        assert!(context.accept_task(CountingTask::new(Arc::clone(&completed))).is_none());
                        Ok::<(), std::convert::Infallible>(())
                    },
                )
                .expect_err("running reporter failure should be returned");

            let BatchExecutionError::ProgressReport { outcome, .. } = error else {
                panic!("running reporter failure should be a progress error");
            };
            assert_eq!(outcome.completed_count(), accepted.load(Ordering::Acquire));
            assert_eq!(completed.load(Ordering::Acquire), accepted.load(Ordering::Acquire));
            assert!(outcome.completed_count() >= 2);
        },
    );
}
