// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`SequentialBatchProcessorBuilder`](qubit_batch::SequentialBatchProcessorBuilder).

use std::time::Duration;

use qubit_batch::SequentialBatchProcessor;

#[test]
fn test_sequential_batch_processor_builder_applies_report_interval() {
    let processor = SequentialBatchProcessor::builder(|_item: &i32| {})
        .report_interval(Duration::ZERO)
        .no_reporter()
        .build();

    assert_eq!(processor.report_interval(), Duration::ZERO);
}
