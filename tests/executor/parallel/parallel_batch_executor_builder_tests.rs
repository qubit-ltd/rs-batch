/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ParallelBatchExecutorBuilder`](qubit_batch::ParallelBatchExecutorBuilder).

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_batch::{
    ParallelBatchExecutor,
    ParallelBatchExecutorBuildError,
    ProgressReporter,
};

use crate::support::RecordingProgressReporter;

#[test]
fn test_parallel_batch_executor_builder_builds_custom_config() {
    let reporter: Arc<dyn ProgressReporter> = Arc::new(RecordingProgressReporter::new());
    let executor = ParallelBatchExecutor::builder()
        .thread_count(3)
        .sequential_threshold(2)
        .report_interval(Duration::from_millis(25))
        .reporter_arc(reporter.clone())
        .build()
        .expect("custom executor should build");

    assert_eq!(executor.thread_count(), 3);
    assert_eq!(executor.sequential_threshold(), 2);
    assert_eq!(executor.report_interval(), Duration::from_millis(25));
    assert!(Arc::ptr_eq(executor.reporter(), &reporter));
}

#[test]
fn test_parallel_batch_executor_builder_can_disable_reporting() {
    let executor = ParallelBatchExecutor::builder()
        .no_reporter()
        .build()
        .expect("executor without reporter should build");

    assert_eq!(
        executor.report_interval(),
        ParallelBatchExecutor::DEFAULT_REPORT_INTERVAL
    );
}

#[test]
fn test_parallel_batch_executor_builder_rejects_invalid_config() {
    assert!(matches!(
        ParallelBatchExecutor::builder().thread_count(0).build(),
        Err(ParallelBatchExecutorBuildError::ZeroThreadCount)
    ));
    assert!(matches!(
        ParallelBatchExecutor::builder()
            .report_interval(Duration::ZERO)
            .build(),
        Err(ParallelBatchExecutorBuildError::ZeroReportInterval)
    ));
}
