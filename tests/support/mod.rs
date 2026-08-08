// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared test support for `qubit-batch`.

mod failing_progress_reporter;
mod progress_reporter;
mod test_callable;
mod test_chunk_processor;
mod test_task;

pub use failing_progress_reporter::FailingReporter;
pub use progress_reporter::PanickingReporter;
pub use progress_reporter::PhaseRecordingReporter;
pub use progress_reporter::ProgressEvent;
pub use progress_reporter::ProgressPanicPhase;
pub use progress_reporter::RecordingReporter;
pub use progress_reporter::panic_payload_message;
pub use test_callable::TestCallable;
pub use test_chunk_processor::TestChunkOutcome;
pub use test_chunk_processor::TestChunkProcessor;
pub use test_task::TestTask;
