// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{sync::Arc, time::Duration};

use qubit_function::{BoxConsumer, Consumer};
use qubit_progress::{Metric, Progress, Reporter};

use crate::ProgressFailure;
use crate::process::{
    BatchProcessError, BatchProcessResult, BatchProcessState, BatchProcessor,
    PROCESS_PROGRESS_METRIC_ID, PROCESS_PROGRESS_METRIC_NAME,
};

use super::SequentialBatchProcessorBuilder;

/// Processes batch items sequentially by invoking a [`Consumer`] per item.
///
/// The processor stores the supplied consumer as a [`BoxConsumer`] and invokes
/// it on the caller thread in input order. Consumer panics are not caught; they
/// propagate to the caller and no [`BatchProcessResult`] is produced. Progress
/// updates are emitted only between items.
///
/// # Type Parameters
///
/// * `Item` - Item type consumed by the stored consumer.
///
/// ```rust
/// use qubit_batch::{
///     BatchProcessor,
///     SequentialBatchProcessor,
/// };
///
/// let mut processor = SequentialBatchProcessor::new(|item: &i32| {
///     assert!(*item > 0);
/// });
///
/// let result = processor
///     .process([1, 2, 3])
///     .expect("array length should be exact");
///
/// assert!(result.is_success());
/// ```
pub struct SequentialBatchProcessor<Item> {
    /// Consumer called once for each accepted item.
    pub(crate) consumer: BoxConsumer<Item>,
    /// Interval between progress callbacks while the batch is running.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn Reporter>,
}

impl<Item> SequentialBatchProcessor<Item> {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Creates a sequential consumer-backed batch processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Consumer invoked once for each input item.
    ///
    /// # Returns
    ///
    /// A processor storing `consumer` as a [`BoxConsumer`].
    #[inline]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + 'static,
    {
        Self::builder(consumer).build()
    }

    /// Creates a builder for configuring a sequential consumer-backed
    /// processor.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Consumer invoked once for each input item.
    ///
    /// # Returns
    ///
    /// A builder initialized with default settings.
    #[inline]
    pub fn builder<C>(consumer: C) -> SequentialBatchProcessorBuilder<Item>
    where
        C: Consumer<Item> + 'static,
    {
        SequentialBatchProcessorBuilder::new(consumer)
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum time between due-based running progress callbacks.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.report_interval
    }

    /// Returns the configured progress reporter.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.reporter
    }

    /// Returns the stored consumer.
    ///
    /// # Returns
    ///
    /// A shared reference to the boxed consumer.
    #[inline]
    pub const fn consumer(&self) -> &BoxConsumer<Item> {
        &self.consumer
    }

    /// Consumes this processor and returns the stored consumer.
    ///
    /// # Returns
    ///
    /// The boxed consumer used by this processor.
    #[inline]
    pub fn into_consumer(self) -> BoxConsumer<Item> {
        self.consumer
    }
}

impl<Item> BatchProcessor<Item> for SequentialBatchProcessor<Item> {
    type Error = BatchProcessError;

    /// Processes items sequentially on the caller thread.
    ///
    /// # Parameters
    ///
    /// * `items` - Item source for the batch.
    /// * `count` - Declared number of items expected from `items`.
    ///
    /// # Returns
    ///
    /// A result with completed and processed counts equal to the number of
    /// consumer calls when the input source yields exactly `count` items.
    ///
    /// # Errors
    ///
    /// Returns [`BatchProcessError::CountShortfall`] when the source ends
    /// before `count`, or [`BatchProcessError::CountExceeded`] when the
    /// source yields an extra item. Extra items are observed but not passed
    /// to the consumer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stored consumer or the configured
    /// progress reporter.
    fn process_with_count<I>(
        &mut self,
        items: I,
        count: usize,
    ) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let mut progress = match Progress::builder(self.reporter.as_ref())
            .interval(self.report_interval)
            .metric(
                Metric::new(PROCESS_PROGRESS_METRIC_ID, PROCESS_PROGRESS_METRIC_NAME)
                    .total(count as u64),
            )
            .start()
        {
            Ok(progress) => progress,
            Err(source) => {
                return Err(BatchProcessError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    result: BatchProcessResult::builder(count)
                        .elapsed(Duration::ZERO)
                        .build()
                        .expect("empty batch process result must be valid"),
                });
            }
        };
        let metric = progress
            .metric(PROCESS_PROGRESS_METRIC_ID)
            .expect("configured process metric must exist");
        let state = BatchProcessState::new(count, metric);

        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                let (elapsed, report_error) = match progress.fail() {
                    Ok(elapsed) => (elapsed, None),
                    Err(source) => {
                        let elapsed = source.elapsed();
                        (elapsed, Some(ProgressFailure::from(source)))
                    }
                };
                let result = state.to_direct_result(elapsed);
                return Err(BatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: observed_count,
                    result,
                    report_error,
                });
            }
            state
                .record_item_started()
                .expect("batch progress state transition must be valid");
            self.consumer.accept(&item);
            state
                .record_item_processed()
                .expect("batch progress state transition must be valid");
            if let Err(source) = progress.report_if_due() {
                return Err(BatchProcessError::ProgressReport {
                    source: Box::new(ProgressFailure::from(source)),
                    result: state.to_direct_result(progress.elapsed()),
                });
            }
        }

        if state.observed_count() < count {
            let (elapsed, report_error) = match progress.fail() {
                Ok(elapsed) => (elapsed, None),
                Err(source) => {
                    let elapsed = source.elapsed();
                    (elapsed, Some(ProgressFailure::from(source)))
                }
            };
            let result = state.to_direct_result(elapsed);
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
                report_error,
            })
        } else {
            let progress_elapsed = progress.elapsed();
            let finished = match progress.finish() {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let failure = ProgressFailure::from_finish_error(source);
                    let elapsed = failure.elapsed().unwrap_or(progress_elapsed);
                    return Err(BatchProcessError::ProgressReport {
                        source: Box::new(failure),
                        result: state.to_direct_result(elapsed),
                    });
                }
            };
            let result = state.to_direct_result(finished);
            Ok(result)
        }
    }
}
