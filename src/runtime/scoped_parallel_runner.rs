/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::panic::resume_unwind;
use std::sync::{
    Arc,
    Mutex,
    mpsc,
};
use std::thread;

/// Indexed work item sent to scoped workers.
struct ScopedWorkItem<T> {
    /// Zero-based item index within the declared batch.
    index: usize,
    /// Work item payload.
    item: T,
}

/// Runs indexed work items on fixed-width scoped worker threads.
///
/// This helper owns only the scoped-thread scheduling template. It deliberately
/// does not update progress, collect failures, catch work-item panics, or build
/// domain results. Callers provide those semantics through `observe_item` and
/// `run_item`.
///
/// # Parameters
///
/// * `items` - Source of work items.
/// * `declared_count` - Declared number of items expected from `items`.
/// * `worker_count` - Number of scoped worker threads to spawn.
/// * `observe_item` - Callback invoked on the producer thread for each observed
///   source item. It must return the observed count after recording the item.
/// * `run_item` - Callback invoked by workers for each accepted item.
///
/// # Returns
///
/// The number of items observed from the source. The runner stops after it
/// observes the first item beyond `declared_count`.
///
/// # Panics
///
/// Panics if `worker_count` is zero. Propagates panics raised by worker threads.
pub(crate) fn run_scoped_parallel<I, T, O, F>(
    items: I,
    declared_count: usize,
    worker_count: usize,
    observe_item: O,
    run_item: F,
) -> usize
where
    I: IntoIterator<Item = T>,
    T: Send,
    O: Fn() -> usize,
    F: Fn(usize, T) + Sync,
{
    assert!(
        worker_count > 0,
        "scoped parallel worker count must be positive"
    );
    let mut observed_count = 0usize;
    thread::scope(|scope| {
        let (work_sender, work_receiver) = mpsc::sync_channel(worker_count);
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_receiver = Arc::clone(&work_receiver);
            let worker_run_item = &run_item;
            worker_handles.push(scope.spawn(move || {
                run_scoped_worker(worker_receiver, worker_run_item);
            }));
        }
        drop(work_receiver);

        for item in items {
            observed_count = observe_item();
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
    observed_count
}

/// Runs one scoped worker until the work channel closes.
///
/// # Parameters
///
/// * `work_receiver` - Shared receiver protected because standard receivers are
///   not `Sync`.
/// * `run_item` - Callback invoked for each accepted work item.
fn run_scoped_worker<T, F>(
    work_receiver: Arc<Mutex<mpsc::Receiver<ScopedWorkItem<T>>>>,
    run_item: &F,
) where
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
        let ScopedWorkItem { index, item } = work_item;
        run_item(index, item);
    }
}
