// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;

use qubit_batch::BatchTaskError;

#[test]
fn test_batch_task_error_failed_and_panicked_helpers() {
    let failed = BatchTaskError::Failed("failed");
    assert!(failed.is_failed());
    assert!(!failed.is_panicked());
    assert_eq!(failed.panic_message(), None);
    assert_eq!(failed.to_string(), "task failed: failed");

    let panicked = BatchTaskError::<&'static str>::panicked("boom");
    assert!(!panicked.is_failed());
    assert!(panicked.is_panicked());
    assert_eq!(panicked.panic_message(), Some("boom"));
    assert_eq!(panicked.to_string(), "task panicked: boom");

    let no_message = BatchTaskError::<&'static str>::panicked_without_message();
    assert_eq!(no_message.panic_message(), None);
    assert_eq!(no_message.to_string(), "task panicked");
}

#[test]
fn test_batch_task_error_builds_from_string_panic_payloads() {
    let payload = catch_unwind(AssertUnwindSafe(|| panic_any("borrowed panic message")))
        .expect_err("panic payload should be captured");
    let error = BatchTaskError::<&'static str>::from_panic_payload(payload.as_ref());
    assert_eq!(error.panic_message(), Some("borrowed panic message"));

    let payload = catch_unwind(AssertUnwindSafe(|| {
        panic_any("owned panic message".to_owned());
    }))
    .expect_err("panic payload should be captured");
    let error = BatchTaskError::<&'static str>::from_panic_payload(payload.as_ref());
    assert_eq!(error.panic_message(), Some("owned panic message"));
}

#[test]
fn test_batch_task_error_builds_from_non_string_panic_payloads() {
    let payload = catch_unwind(AssertUnwindSafe(|| panic_any(7usize)))
        .expect_err("panic payload should be captured");
    let error = BatchTaskError::<&'static str>::from_panic_payload(payload.as_ref());
    assert!(error.is_panicked());
    assert_eq!(error.panic_message(), None);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[test]
fn test_batch_task_error_helpers_display_and_source() {
    let failed = BatchTaskError::Failed(TestError("failed"));
    assert!(failed.is_failed());
    assert!(!failed.is_panicked());
    assert_eq!(failed.to_string(), "task failed: failed");
    assert_eq!(failed.source().expect("source").to_string(), "failed");

    let panicked = BatchTaskError::<TestError>::panicked("panic");
    assert!(!panicked.is_failed());
    assert!(panicked.is_panicked());
    assert_eq!(panicked.panic_message(), Some("panic"));
    assert_eq!(panicked.to_string(), "task panicked: panic");
    assert!(panicked.source().is_none());

    let panicked_without_message = BatchTaskError::<TestError>::panicked_without_message();
    assert_eq!(panicked_without_message.panic_message(), None);
    assert_eq!(panicked_without_message.to_string(), "task panicked");
}
