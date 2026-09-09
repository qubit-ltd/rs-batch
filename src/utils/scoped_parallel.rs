// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;

use super::internal::ScopedWorkItem;

/// Runs indexed work items on fixed-width scoped worker threads.
///
/// This helper owns only the scoped-thread scheduling template. It deliberately
/// does not update progress, collect failures, catch work-item panics, or build
/// domain results. Callers provide those semantics through `observe_item` and
/// `run_item`. The `should_stop` callback enables cooperative cancellation
/// after an external terminal condition, such as a failed progress reporter.
///
/// # Parameters
///
/// * `items` - Source of work items.
/// * `declared_count` - Declared number of items expected from `items`.
/// * `worker_count` - Number of scoped worker threads to spawn.
/// * `observe_item` - Callback invoked on the producer thread for each observed
///   source item. It must return the observed count after recording the item.
/// * `should_stop` - Callback checked before accepting or executing work.
/// * `run_item` - Callback invoked by workers for each accepted item.
///
/// # Type Parameters
///
/// * `I` - Source iterator type.
/// * `T` - Source item type.
/// * `O` - Observation callback type.
/// * `S` - Stop predicate type.
/// * `F` - Worker callback type.
///
/// The observer records every pulled item up to the first item beyond
/// `declared_count`; that extra item is not passed to a worker.
///
/// # Panics
///
/// Panics if `worker_count` is zero. Propagates panics raised by worker
/// threads.
pub(crate) fn run_scoped_parallel<I, T, O, S, F>(
    items: I,
    declared_count: usize,
    worker_count: usize,
    observe_item: O,
    should_stop: S,
    run_item: F,
) where
    I: IntoIterator<Item = T>,
    T: Send,
    O: Fn() -> usize,
    S: Fn() -> bool + Sync,
    F: Fn(usize, T) + Sync,
{
    assert!(worker_count > 0, "scoped parallel worker count must be positive");
    thread::scope(|scope| {
        let (work_sender, work_receiver) = mpsc::sync_channel(worker_count);
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_receiver = Arc::clone(&work_receiver);
            let worker_should_stop = &should_stop;
            let worker_run_item = &run_item;
            worker_handles.push(scope.spawn(move || {
                run_scoped_worker(worker_receiver, worker_should_stop, worker_run_item);
            }));
        }
        drop(work_receiver);

        for item in items {
            if should_stop() {
                break;
            }
            let observed_count = observe_item();
            if observed_count > declared_count {
                break;
            }
            if work_sender
                .send(ScopedWorkItem {
                    index: observed_count - 1,
                    item,
                })
                .is_err()
            {
                break;
            }
        }
        drop(work_sender);

        for handle in worker_handles {
            if let Err(payload) = handle.join() {
                resume_unwind(payload);
            }
        }
    });
}

/// Runs accepted work items on fixed-width scoped worker threads.
///
/// The acceptance callback owns admission control, including cancellation and
/// task-count validation. Only accepted work is placed on the bounded channel.
///
/// # Parameters
///
/// * `items` - Source of runtime-specific work items.
/// * `worker_count` - Number of scoped worker threads to spawn.
/// * `accept_item` - Converts an item into an accepted work token, or returns
///   `None` to stop consuming the source.
/// * `run_item` - Executes one accepted work token on a worker.
///
/// # Type Parameters
///
/// * `I` - Source iterator type.
/// * `T` - Source item type.
/// * `W` - Accepted work token type.
/// * `A` - Admission callback type.
/// * `F` - Worker callback type.
pub(crate) fn run_scoped_parallel_tasks<I, T, W, A, F>(items: I, worker_count: usize, accept_item: A, run_item: F)
where
    I: IntoIterator<Item = T>,
    T: Send,
    W: Send,
    A: Fn(T) -> Option<W>,
    F: Fn(W) + Sync,
{
    assert!(worker_count > 0, "scoped parallel worker count must be positive");
    thread::scope(|scope| {
        let (work_sender, work_receiver) = mpsc::sync_channel(worker_count);
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_receiver = Arc::clone(&work_receiver);
            let worker_run_item = &run_item;
            worker_handles.push(scope.spawn(move || {
                run_scoped_task_worker(worker_receiver, worker_run_item);
            }));
        }
        drop(work_receiver);

        for item in items {
            let Some(work) = accept_item(item) else {
                break;
            };
            if work_sender.send(work).is_err() {
                break;
            }
        }
        drop(work_sender);

        for handle in worker_handles {
            if let Err(payload) = handle.join() {
                resume_unwind(payload);
            }
        }
    });
}

/// Runs one scoped worker until the work channel closes.
///
/// # Parameters
///
/// * `work_receiver` - Shared receiver protected because standard receivers are
///   not `Sync`.
/// * `run_item` - Callback invoked for each accepted work item.
///
/// # Type Parameters
///
/// * `T` - Work item payload type.
/// * `S` - Stop predicate type.
/// * `F` - Worker callback type.
fn run_scoped_worker<T, S, F>(
    work_receiver: Arc<Mutex<mpsc::Receiver<ScopedWorkItem<T>>>>,
    should_stop: &S,
    run_item: &F,
) where
    S: Fn() -> bool,
    F: Fn(usize, T),
{
    loop {
        let received = work_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        let Ok(work_item) = received else {
            break;
        };
        if should_stop() {
            break;
        }
        let ScopedWorkItem { index, item } = work_item;
        run_item(index, item);
    }
}

/// Runs accepted work until the token channel closes.
///
/// # Type Parameters
///
/// * `W` - Accepted work token type.
/// * `F` - Worker callback type.
fn run_scoped_task_worker<W, F>(work_receiver: Arc<Mutex<mpsc::Receiver<W>>>, run_item: &F)
where
    F: Fn(W),
{
    loop {
        let received = work_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        let Ok(work) = received else {
            break;
        };
        run_item(work);
    }
}
