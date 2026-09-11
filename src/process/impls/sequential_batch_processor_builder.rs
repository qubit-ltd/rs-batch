// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use qubit_function::BoxConsumer;
use qubit_function::Consumer;
use qubit_progress::reporter::NoopReporter;
use qubit_progress::reporter::Reporter;

use super::SequentialBatchProcessor;

/// Builder for [`SequentialBatchProcessor`].
///
/// Use the builder when the default progress interval or reporter should be
/// customized.
///
/// # Type Parameters
///
/// * `Item` - Item type consumed by the processor being built.
/// * `C` - Stored consumer type. The default is [`BoxConsumer<Item>`].
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_batch::SequentialBatchProcessor;
///
/// let processor = SequentialBatchProcessor::builder(|_item: &i32| {})
///     .report_interval(Duration::ZERO)
///     .build();
///
/// assert_eq!(processor.report_interval(), Duration::ZERO);
/// ```
#[must_use = "configure and build the value before discarding this builder"]
pub struct SequentialBatchProcessorBuilder<Item, C = BoxConsumer<Item>> {
    /// Consumer called once for each accepted item.
    consumer: C,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn Reporter>,
    /// Associates the consumed item type without owning an item.
    item_marker: PhantomData<fn(&Item)>,
}

impl<Item> SequentialBatchProcessorBuilder<Item> {
    /// Creates a builder from a consumer.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Consumer type stored by the builder.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Consumer invoked once for each input item.
    ///
    /// # Returns
    ///
    /// A builder initialized with default sequential processor settings.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub fn new<C>(consumer: C) -> Self
    where
        C: Consumer<Item> + 'static,
    {
        Self {
            consumer: BoxConsumer::new(consumer),
            report_interval: SequentialBatchProcessor::<Item>::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoopReporter),
            item_marker: PhantomData,
        }
    }

    /// Creates a builder that stores a consumer directly.
    ///
    /// This constructor accepts borrowed and non-`Send` consumers because the
    /// sequential processor invokes the consumer only on the caller thread.
    ///
    /// # Parameters
    ///
    /// * `consumer` - Consumer invoked once for each input item.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Consumer type stored by the builder.
    ///
    /// # Returns
    ///
    /// A builder initialized with default sequential processor settings.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub fn with_consumer<C>(consumer: C) -> SequentialBatchProcessorBuilder<Item, C>
    where
        C: Consumer<Item>,
    {
        SequentialBatchProcessorBuilder {
            consumer,
            report_interval: SequentialBatchProcessor::<Item>::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoopReporter),
            item_marker: PhantomData,
        }
    }
}

impl<Item, C> SequentialBatchProcessorBuilder<Item, C> {
    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum time between due-based running progress
    ///   callbacks. [`Duration::ZERO`] reports at every sequential between-item
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

    /// Builds a [`SequentialBatchProcessor`].
    ///
    /// # Returns
    ///
    /// A sequential batch processor with this builder's configuration.
    #[inline]
    pub fn build(self) -> SequentialBatchProcessor<Item, C> {
        SequentialBatchProcessor {
            consumer: self.consumer,
            report_interval: self.report_interval,
            reporter: self.reporter,
            item_marker: PhantomData,
        }
    }
}
