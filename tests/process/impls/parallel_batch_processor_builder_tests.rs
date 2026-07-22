// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ParallelBatchProcessorBuilder`](qubit_batch::ParallelBatchProcessorBuilder).

use std::time::Duration;

use qubit_batch::ParallelBatchProcessor;

#[test]
fn test_parallel_batch_processor_builder_applies_configuration() {
    let processor = ParallelBatchProcessor::builder(|_item: &i32| {})
        .thread_count(2)
        .sequential_threshold(3)
        .report_interval(Duration::ZERO)
        .no_reporter()
        .build()
        .expect("valid parallel processor should build");

    assert_eq!(processor.thread_count(), 2);
    assert_eq!(processor.sequential_threshold(), 3);
    assert_eq!(processor.report_interval(), Duration::ZERO);
}
