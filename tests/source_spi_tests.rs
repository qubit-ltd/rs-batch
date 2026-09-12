use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::NoopReporter;

fn coordinator() -> ParallelBatchExecutionCoordinator {
    ParallelBatchExecutionCoordinator::new(Arc::new(NoopReporter), Duration::ZERO)
}

#[test]
fn source_consumption_proves_exhaustion() {
    let outcome = coordinator()
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                for token in source.by_ref() {
                    context.execute_task(token);
                }
                Ok::<(), Infallible>(())
            },
        )
        .expect("consuming source to None should complete the schedule");
    assert!(outcome.is_success());
}

#[test]
fn source_scheduler_return_without_exhaustion_is_incomplete() {
    let error = coordinator()
        .execute_with_source(
            [|| Ok::<(), ()>(())],
            1,
            TaskFailurePolicy::Continue,
            |source, context| {
                context.execute_task(source.next().expect("declared task should exist"));
                Ok::<(), Infallible>(())
            },
        )
        .expect_err("the source must be exhausted explicitly");
    assert!(error.is_incomplete_schedule());
    assert_eq!(error.outcome().completed_count(), 1);
}

#[test]
fn source_is_fused_after_admission_stop() {
    let outcome = coordinator()
        .execute_with_source(
            std::iter::repeat_with(|| || Err::<(), _>("stop")),
            8,
            TaskFailurePolicy::StopOnFirstFailure,
            |source, context| {
                if let Some(token) = source.next() {
                    context.execute_task(token);
                }
                assert!(source.next().is_none());
                Ok::<(), Infallible>(())
            },
        )
        .expect("policy stop returns an outcome");
    assert_eq!(outcome.failed_count(), 1);
}
