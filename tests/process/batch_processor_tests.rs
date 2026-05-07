/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::ChunkedBatchProcessor;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::support::TestChunkProcessor;

#[test]
fn test_batch_processor_chunked_accessors() {
    let processor = ChunkedBatchProcessor::new(
        TestChunkProcessor::success(),
        NonZeroUsize::new(3).expect("non-zero chunk size"),
    )
    .with_report_interval(Duration::from_millis(25));

    assert_eq!(processor.chunk_size().get(), 3);
    assert_eq!(processor.report_interval(), Duration::from_millis(25));
}
