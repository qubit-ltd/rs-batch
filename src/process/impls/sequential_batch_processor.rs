/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    sync::Arc,
    time::Duration,
};

use qubit_function::{
    BoxConsumer,
    Consumer,
};
use qubit_progress::{
    Progress,
    model::ProgressPhase,
    reporter::{
        NoOpProgressReporter,
        ProgressReporter,
    },
};

use crate::process::{
    BatchProcessError,
    BatchProcessResult,
    BatchProcessState,
    BatchProcessor,
};

/// Processes batch items sequentially by invoking a [`Consumer`] per item.
///
/// The processor stores the supplied consumer as a [`BoxConsumer`] and invokes it
/// on the caller thread in input order. Consumer panics are not caught; they
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
///     .process([1, 2, 3], 3)
///     .expect("iterator should yield exactly three items");
///
/// assert!(result.is_success());
/// ```
pub struct SequentialBatchProcessor<Item> {
    /// Consumer called once for each accepted item.
    consumer: BoxConsumer<Item>,
    /// Interval between progress callbacks while the batch is running.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
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
        Self {
            consumer: consumer.into_box(),
            report_interval: Self::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter<R>(self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.with_reporter_arc(Arc::new(reporter))
    }

    /// Returns a copy configured with the supplied progress reporter.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This processor configured with `reporter`.
    #[inline]
    pub fn with_reporter_arc(self, reporter: Arc<dyn ProgressReporter>) -> Self {
        Self { reporter, ..self }
    }

    /// Returns a copy configured with the supplied progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every sequential
    ///   between-item progress point.
    ///
    /// # Returns
    ///
    /// This processor configured with `report_interval`.
    #[inline]
    pub fn with_report_interval(self, report_interval: Duration) -> Self {
        Self {
            report_interval,
            ..self
        }
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
    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
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
    /// Returns [`BatchProcessError::CountShortfall`] when the source ends before
    /// `count`, or [`BatchProcessError::CountExceeded`] when the source yields an
    /// extra item. Extra items are observed but not passed to the consumer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stored consumer or the configured
    /// progress reporter.
    fn process<I>(&mut self, items: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let state = BatchProcessState::new(count);
        let mut progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            state.progress_counters(),
            Duration::ZERO,
        );

        for item in items {
            let observed_count = state.record_item_observed();
            if observed_count > count {
                let result = state.to_direct_result(progress.elapsed());
                progress.report_with_elapsed(
                    ProgressPhase::Failed,
                    state.progress_counters(),
                    result.elapsed(),
                );
                return Err(BatchProcessError::CountExceeded {
                    expected: count,
                    observed_at_least: observed_count,
                    result,
                });
            }
            state.record_item_started();
            self.consumer.accept(&item);
            state.record_item_processed();
            progress.report_running_if_due(state.progress_counters());
        }

        let result = state.to_direct_result(progress.elapsed());
        if state.observed_count() < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                state.progress_counters(),
                result.elapsed(),
            );
            Err(BatchProcessError::CountShortfall {
                expected: count,
                actual: state.observed_count(),
                result,
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                state.progress_counters(),
                result.elapsed(),
            );
            Ok(result)
        }
    }
}
