// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// One successful callable value paired with its original source index.
///
/// Both [`crate::BatchCallResult`] and [`crate::BatchCallError`] retain sparse
/// vectors of these outputs. Failed, panicked, and unexecuted callables have no
/// entry; indexes need not be contiguous.
///
/// # Type Parameters
///
/// * `R` - Callable success value type.
///
/// # Examples
///
/// ```rust
/// use qubit_batch::BatchCallOutput;
/// let output = BatchCallOutput::new(3, "accepted");
/// assert_eq!(output.index(), 3);
/// assert_eq!(output.into_parts(), (3, "accepted"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCallOutput<R> {
    /// Zero-based callable index that produced `value`.
    index: usize,
    /// Successful value produced by the callable.
    value: R,
}

impl<R> BatchCallOutput<R> {
    /// Creates one indexed callable output.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based callable index.
    /// * `value` - Successful callable value.
    ///
    /// # Returns
    ///
    /// An indexed callable output.
    #[inline]
    #[must_use = "use the constructed or borrowed value"]
    pub const fn new(index: usize, value: R) -> Self {
        Self { index, value }
    }

    /// Returns the zero-based callable index.
    ///
    /// # Returns
    ///
    /// The original position of the successful callable.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns a reference to the successful callable value.
    ///
    /// # Returns
    ///
    /// The value produced by the callable.
    #[must_use = "inspect the returned value"]
    #[inline]
    pub const fn value(&self) -> &R {
        &self.value
    }

    /// Consumes the output and returns its successful value.
    ///
    /// # Returns
    ///
    /// The callable value without its index.
    #[inline]
    pub fn into_value(self) -> R {
        self.value
    }

    /// Consumes the output and returns its index and value.
    ///
    /// # Returns
    ///
    /// A tuple containing the original index and successful value.
    #[inline]
    pub fn into_parts(self) -> (usize, R) {
        (self.index, self.value)
    }
}
