// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::BatchCallOutput;
use crate::BatchCallResultBuildError;
use crate::BatchOutcome;

/// Result produced by [`crate::BatchExecutor::call`].
///
/// Successful callable outputs are retained sparsely with their original
/// indexes. This keeps early-stop results bounded by the number of completed
/// callables instead of the declared task count.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "batch call results contain execution failures and returned values"]
pub struct BatchCallResult<R, E> {
    /// Execution outcome and failures for the callable batch.
    outcome: BatchOutcome<E>,
    /// Successful outputs sorted by callable position.
    outputs: Vec<BatchCallOutput<R>>,
}

impl<R, E> BatchCallResult<R, E> {
    /// Creates a new callable batch result from sparse successful outputs.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCallResultBuildError`] when output ordering, indexes, or
    /// count disagrees with `outcome`.
    pub fn try_new(
        outcome: BatchOutcome<E>,
        outputs: Vec<BatchCallOutput<R>>,
    ) -> Result<Self, BatchCallResultBuildError> {
        let mut previous_index = None;
        for output in &outputs {
            if let Some(previous_index) = previous_index
                && output.index() <= previous_index
            {
                return Err(BatchCallResultBuildError::OutputIndexOutOfOrder {
                    previous_index,
                    index: output.index(),
                });
            }
            previous_index = Some(output.index());
            if output.index() >= outcome.task_count() {
                return Err(BatchCallResultBuildError::OutputIndexOutOfRange {
                    index: output.index(),
                    task_count: outcome.task_count(),
                });
            }
        }
        for failure in outcome.failures() {
            if outputs.iter().any(|output| output.index() == failure.index()) {
                return Err(BatchCallResultBuildError::FailureOutputPresent { index: failure.index() });
            }
        }
        if outputs.len() != outcome.succeeded_count() {
            return Err(BatchCallResultBuildError::SucceededOutputCountMismatch {
                succeeded_count: outcome.succeeded_count(),
                output_count: outputs.len(),
            });
        }
        Ok(Self { outcome, outputs })
    }

    /// Returns the execution outcome for the callable batch.
    #[inline]
    pub const fn outcome(&self) -> &BatchOutcome<E> {
        &self.outcome
    }

    /// Returns sparse successful outputs sorted by callable position.
    #[inline]
    pub fn outputs(&self) -> &[BatchCallOutput<R>] {
        &self.outputs
    }

    /// Consumes this result and returns the execution outcome.
    #[inline]
    pub fn into_outcome(self) -> BatchOutcome<E> {
        self.outcome
    }

    /// Consumes this result and returns sparse successful outputs.
    #[inline]
    pub fn into_outputs(self) -> Vec<BatchCallOutput<R>> {
        self.outputs
    }

    /// Consumes this result and returns both stored parts.
    #[inline]
    pub fn into_parts(self) -> (BatchOutcome<E>, Vec<BatchCallOutput<R>>) {
        (self.outcome, self.outputs)
    }
}
