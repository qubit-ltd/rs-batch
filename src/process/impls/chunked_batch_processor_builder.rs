// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use qubit_progress::reporter::NoopReporter;
use qubit_progress::reporter::Reporter;

use super::ChunkedBatchProcessor;

/// Builder for [`ChunkedBatchProcessor`].
///
/// Use the builder when the default progress interval or reporter should be
/// customized.
///
/// # Type Parameters
///
/// * `P` - Processor that receives each collected chunk.
///
/// # Examples
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
#[must_use = "configure and build the value before discarding this builder"]
pub struct ChunkedBatchProcessorBuilder<P> {
    /// Delegate processor receiving each chunk.
    delegate: P,
    /// Maximum number of items submitted to the delegate at once.
    chunk_size: NonZeroUsize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn Reporter>,
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
    #[must_use = "use the constructed or borrowed value"]
    pub fn new(delegate: P, chunk_size: NonZeroUsize) -> Self {
        Self {
            delegate,
            chunk_size,
            report_interval: ChunkedBatchProcessor::<P>::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoopReporter),
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
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub const fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by built processors.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Concrete reporter type stored behind the shared reporter trait
    ///   object.
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
        R: Reporter + 'static,
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
    #[must_use = "inspect the returned value"]
    #[inline(always)]
    pub fn reporter_arc(mut self, reporter: Arc<dyn Reporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Disables progress callbacks by using [`NoopReporter`].
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoopReporter);
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
