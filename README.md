# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Batch-oriented task execution abstractions and sequential utilities for the
Qubit Rust libraries.

## Overview

Qubit Batch focuses on one-shot execution of whole task batches instead of
single-task submission. The crate provides:

- `BatchExecutor`: trait for executing a batch of fallible runnable tasks.
- `SequentialBatchExecutor`: deterministic, in-order execution on the caller
  thread.
- `ProgressReporter`: pluggable progress callbacks for start, in-flight
  progress, and finish notifications.
- `BatchExecutionResult`: structured batch outcome with failure aggregation and
  elapsed-time reporting.

Rayon-backed parallel batch execution lives in the companion
`qubit-rayon-batch` crate.

## Features

- Accept eager or lazy task sources through `IntoIterator`.
- Keep batch-level statistics for completed, succeeded, failed, and panicked
  tasks.
- Record per-task failures with stable batch indexes and readable panic
  messages.
- Keep the core API free of runtime-specific dependencies.

## Installation

```toml
[dependencies]
qubit-batch = "0.3.0"
```

## Quick Start

### Sequential execution

```rust
use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};

let executor = SequentialBatchExecutor::new();
let tasks = vec![
    || Ok::<(), &'static str>(()),
    || Ok::<(), &'static str>(()),
];

let result = executor.execute(tasks, 2).expect("batch should succeed");
assert_eq!(result.task_count(), 2);
assert_eq!(result.completed_count(), 2);
assert_eq!(result.succeeded_count(), 2);
```
