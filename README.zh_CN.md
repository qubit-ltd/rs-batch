# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Documentation](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的批量任务执行工具 crate。

## 概述

Qubit Batch 关注的是“整批任务”的一次性执行，而不是单个任务的提交。
它提供：

- `BatchExecutor`：执行一批可失败 `Runnable` 任务的 trait。
- `SequentialBatchExecutor`：在调用线程中按顺序执行任务。
- `ParallelBatchExecutor`：基于 Rayon 的并行批量执行器，支持并行度配置和阈值退化。
- `ProgressReporter`：可插拔的进度回调接口，支持开始、处理中和完成通知。
- `BatchExecutionResult`：结构化的批处理结果，包含失败聚合和耗时信息。

## 特性

- 通过 `IntoIterator` 同时支持急切和惰性的任务源。
- 维护批次级统计信息：完成、成功、失败、panic 等计数。
- 用稳定的任务索引记录失败项，并保留可读的 panic 消息，便于定位。
- 使用 Rayon 执行 CPU 密集型并行批处理。
- 对小批量任务自动退化到顺序执行，减少并行化开销。

## 安装

```toml
[dependencies]
qubit-batch = "0.1.0"
```

## 快速开始

### 顺序执行

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

### 并行执行

```rust
use qubit_batch::{
    BatchExecutor,
    ParallelBatchExecutor,
};

let executor = ParallelBatchExecutor::builder()
    .parallelism(4)
    .parallel_threshold(1)
    .build()
    .expect("parallel executor should be created");

let tasks = (0..8).map(|_| || Ok::<(), &'static str>(()));
let result = executor.execute(tasks, 8).expect("batch should succeed");

assert_eq!(result.completed_count(), 8);
assert_eq!(result.failure_count(), 0);
```
