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

use qubit_batch::execute::impls::{
    SequentialBatchExecutor as ExecuteSequentialBatchExecutor,
    SequentialBatchExecutorBuilder as ExecuteSequentialBatchExecutorBuilder,
};
use qubit_batch::execute::{
    BatchExecutor as ExecuteBatchExecutor,
    BatchOutcome as ExecuteBatchOutcome,
    SequentialBatchExecutorBuilder as ExecuteModuleSequentialBatchExecutorBuilder,
};
use qubit_batch::process::impls::{
    ChunkedBatchProcessorBuilder as ProcessImplChunkedBatchProcessorBuilder,
    SequentialBatchProcessor as ProcessSequentialBatchProcessor,
    SequentialBatchProcessorBuilder as ProcessImplSequentialBatchProcessorBuilder,
};
use qubit_batch::process::{
    BatchProcessResult as ProcessBatchProcessResult,
    BatchProcessResultBuildError as ProcessBatchProcessResultBuildError,
    BatchProcessResultBuilder as ProcessBatchProcessResultBuilder,
    BatchProcessor as ProcessBatchProcessor,
    ChunkedBatchProcessorBuilder as ProcessChunkedBatchProcessorBuilder,
    ParallelBatchProcessorBuildError as ProcessParallelBatchProcessorBuildError,
    SequentialBatchProcessorBuilder as ProcessSequentialBatchProcessorBuilder,
};
use qubit_batch::{
    BatchExecutor,
    BatchOutcome,
    BatchProcessResult,
    BatchProcessResultBuildError,
    BatchProcessResultBuilder,
    BatchProcessor,
    ChunkedBatchProcessorBuilder,
    ParallelBatchProcessorBuildError,
    SequentialBatchExecutor,
    SequentialBatchExecutorBuilder,
    SequentialBatchProcessor,
    SequentialBatchProcessorBuilder,
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
    let _root_parallel_processor_build_error: Option<ParallelBatchProcessorBuildError> = None;
    let _process_parallel_processor_build_error: Option<ProcessParallelBatchProcessorBuildError> =
        None;
    let _root_sequential_executor_builder: Option<SequentialBatchExecutorBuilder> = None;
    let _execute_sequential_executor_builder: Option<ExecuteSequentialBatchExecutorBuilder> = None;
    let _execute_module_sequential_executor_builder: Option<
        ExecuteModuleSequentialBatchExecutorBuilder,
    > = None;
    let _root_sequential_processor_builder: Option<SequentialBatchProcessorBuilder<i32>> = None;
    let _process_sequential_processor_builder: Option<ProcessSequentialBatchProcessorBuilder<i32>> =
        None;
    let _process_impl_sequential_processor_builder: Option<
        ProcessImplSequentialBatchProcessorBuilder<i32>,
    > = None;
    let _root_chunked_processor_builder: Option<
        ChunkedBatchProcessorBuilder<SequentialBatchProcessor<i32>>,
    > = None;
    let _process_chunked_processor_builder: Option<
        ProcessChunkedBatchProcessorBuilder<SequentialBatchProcessor<i32>>,
    > = None;
    let _process_impl_chunked_processor_builder: Option<
        ProcessImplChunkedBatchProcessorBuilder<SequentialBatchProcessor<i32>>,
    > = None;
}
