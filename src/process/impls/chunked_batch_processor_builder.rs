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
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use qubit_progress::reporter::{
    NoOpProgressReporter,
    ProgressReporter,
};

use super::ChunkedBatchProcessor;

/// Builder for [`ChunkedBatchProcessor`].
///
/// Use the builder when the default progress interval or reporter should be
/// customized.
///
/// ```rust
/// use std::{
///     num::NonZeroUsize,
///     time::Duration,
/// };
///
/// use qubit_batch::{
///     ChunkedBatchProcessor,
///     SequentialBatchProcessor,
/// };
///
/// let delegate = SequentialBatchProcessor::new(|_item: &i32| {});
/// let processor = ChunkedBatchProcessor::builder(
///     delegate,
///     NonZeroUsize::new(2).expect("chunk size should be non-zero"),
/// )
/// .report_interval(Duration::ZERO)
/// .build();
///
/// assert_eq!(processor.chunk_size().get(), 2);
/// assert_eq!(processor.report_interval(), Duration::ZERO);
/// ```
pub struct ChunkedBatchProcessorBuilder<P> {
    /// Delegate processor receiving each chunk.
    delegate: P,
    /// Maximum number of items submitted to the delegate at once.
    chunk_size: NonZeroUsize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
}

impl<P> ChunkedBatchProcessorBuilder<P> {
    /// Creates a builder from a delegate and chunk size.
    ///
    /// # Parameters
    ///
    /// * `delegate` - Processor receiving each chunk.
    /// * `chunk_size` - Maximum number of items submitted in one chunk.
    ///
    /// # Returns
    ///
    /// A builder initialized with default chunked processor settings.
    #[inline]
    pub fn new(delegate: P, chunk_size: NonZeroUsize) -> Self {
        Self {
            delegate,
            chunk_size,
            report_interval: ChunkedBatchProcessor::<P>::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
        }
    }

    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every completed-chunk
    ///   progress point.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by built processors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter<R>(mut self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.reporter = Arc::new(reporter);
        self
    }

    /// Sets the shared progress reporter used by built processors.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared progress reporter used for later processing calls.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter_arc(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Disables progress callbacks by using [`NoOpProgressReporter`].
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoOpProgressReporter);
        self
    }

    /// Builds a [`ChunkedBatchProcessor`].
    ///
    /// # Returns
    ///
    /// A chunked batch processor with this builder's configuration.
    #[inline]
    pub fn build(self) -> ChunkedBatchProcessor<P> {
        ChunkedBatchProcessor {
            delegate: self.delegate,
            chunk_size: self.chunk_size,
            report_interval: self.report_interval,
            reporter: self.reporter,
        }
    }
}
