/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for the crate-level public API layout.

use qubit_batch::execute::impls::SequentialBatchExecutor as ExecuteSequentialBatchExecutor;
use qubit_batch::execute::{
    BatchExecutor as ExecuteBatchExecutor,
    BatchOutcome as ExecuteBatchOutcome,
};
use qubit_batch::process::impls::SequentialBatchProcessor as ProcessSequentialBatchProcessor;
use qubit_batch::process::{
    BatchProcessResult as ProcessBatchProcessResult,
    BatchProcessResultBuildError as ProcessBatchProcessResultBuildError,
    BatchProcessResultBuilder as ProcessBatchProcessResultBuilder,
    BatchProcessor as ProcessBatchProcessor,
};
use qubit_batch::{
    BatchExecutor,
    BatchOutcome,
    BatchProcessResult,
    BatchProcessResultBuildError,
    BatchProcessResultBuilder,
    BatchProcessor,
    SequentialBatchExecutor,
    SequentialBatchProcessor,
};

#[test]
fn test_core_types_are_exported_from_crate_root_and_grouped_modules() {
    fn assert_root_executor<E: BatchExecutor>() {}
    fn assert_execute_executor<E: ExecuteBatchExecutor>() {}
    fn assert_root_processor<P: BatchProcessor<i32>>() {}
    fn assert_process_processor<P: ProcessBatchProcessor<i32>>() {}

    assert_root_executor::<SequentialBatchExecutor>();
    assert_execute_executor::<ExecuteSequentialBatchExecutor>();
    assert_root_processor::<SequentialBatchProcessor<i32>>();
    assert_process_processor::<ProcessSequentialBatchProcessor<i32>>();

    let _root_outcome: Option<BatchOutcome<&'static str>> = None;
    let _execute_outcome: Option<ExecuteBatchOutcome<&'static str>> = None;
    let _root_process_result: Option<BatchProcessResult> = None;
    let _process_result: Option<ProcessBatchProcessResult> = None;
    let _root_process_result_builder = BatchProcessResultBuilder::builder(0);
    let _process_result_builder = ProcessBatchProcessResultBuilder::builder(0);
    let _root_process_result_build_error: Option<BatchProcessResultBuildError> = None;
    let _process_result_build_error: Option<ProcessBatchProcessResultBuildError> = None;
}
