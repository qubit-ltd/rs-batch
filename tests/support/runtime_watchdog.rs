// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Killable watchdog for tests that coordinate blocking worker threads.

use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use std::time::Instant;

const CHILD_ENV: &str = "QUBIT_BATCH_PARALLEL_TEST_CHILD";
const TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a crate-qualified test scenario in a killable child process.
pub fn run_with_watchdog(test_path: &str, scenario: impl FnOnce()) {
    let (_, test_name) = test_path
        .split_once("::")
        .expect("test path should include its integration crate prefix");
    if std::env::var(CHILD_ENV).ok().as_deref() == Some(test_name) {
        scenario();
        return;
    }
    let mut child = Command::new(std::env::current_exe().expect("test executable should exist"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_ENV, test_name)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("test child should start");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("child status should be readable") {
            assert!(status.success(), "test child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("timed-out child should be killable");
            let _ = child.wait();
            panic!("test child exceeded {TIMEOUT:?}: {test_name}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}
