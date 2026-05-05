/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::NoOpProgressReporter;
use std::time::Duration;

#[test]
fn test_no_op_progress_reporter_methods_do_not_panic() {
    let reporter = NoOpProgressReporter;

    reporter.start(2);
    reporter.process(2, 1, 1, Duration::from_millis(5));
    reporter.finish(2, Duration::from_millis(10));

    assert_eq!(reporter, NoOpProgressReporter);
}
