/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

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
