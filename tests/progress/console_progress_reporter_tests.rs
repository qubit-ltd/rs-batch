/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use qubit_batch::progress::ConsoleProgressReporter;
use std::time::Duration;

#[test]
fn test_console_progress_reporter_can_report_lifecycle() {
    let reporter = ConsoleProgressReporter::new();
    reporter.start(0);
    reporter.process(0, 0, 0, Duration::ZERO);
    reporter.finish(0, Duration::ZERO);

    let reporter = ConsoleProgressReporter::default();
    reporter.start(1);
    reporter.finish(1, Duration::from_secs(1));
}
