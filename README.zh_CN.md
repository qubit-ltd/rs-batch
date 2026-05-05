# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Documentation](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的批量任务执行抽象和顺序执行工具 crate。

## 适用场景

当你已经有一组有限的、可能失败的任务，并希望把它们作为一个批次执行、
统计和诊断时，可以使用 `qubit-batch`：

- 数据导入或校验任务：希望每条记录都被尝试处理；
- 运维脚本：需要在结束时拿到成功、失败和 panic 的汇总；
- 批处理流水线：需要稳定的任务下标来定位失败项或后续重试；
- 公共库代码：不希望核心抽象绑定到 Tokio、Rayon 或其他具体运行时。

这个 crate 不是队列、调度器、工作线程池或重试框架。它只对调用者提供的
批次执行一次，并返回结构化结果。

## 概述

Qubit Batch 关注的是“整批任务”的一次性执行，而不是单个任务的提交。
它提供：

- `BatchExecutor`：执行一批可失败 `Runnable` 任务的 trait。
- `BatchExecutor::call`：执行一批可失败 `Callable`，并按下标收集成功返回值的
  便捷接口。
- `BatchProcessor`：直接处理数据项批次的 trait，不要求先把每个数据项包装成任务。
- `ChunkedBatchProcessor`：按固定大小拆分逻辑批次，并把每个 chunk 提交给代理
  processor 的适配器。
- `SequentialBatchExecutor`：在调用线程中按顺序执行任务。
- `ProgressReporter`：可插拔的进度回调接口，支持开始、处理中和完成通知。
- `WriterProgressReporter` 和 `LoggerProgressReporter`：分别输出到 stdout 和
  `log` crate 的具体进度上报器。
- `BatchOutcome`：结构化的批处理结果，包含失败聚合和单调耗时信息。

基于 Rayon 的并行批量执行器位于配套的 `qubit-rayon-batch` crate。

## 特性

- 通过 `IntoIterator` 同时支持急切和惰性的任务源。
- 维护批次级统计信息：完成、成功、失败、panic 等计数。
- 用稳定的任务索引记录失败项，并保留可读的 panic 消息，便于定位。
- 任务自身失败不会中断整个批次，而是作为结果数据聚合返回。
- 能检测声明任务数与迭代器实际产出数量不一致的情况，并携带已经收集到的
  部分执行结果。
- 核心 API 不依赖具体运行时。

## 安装

```toml
[dependencies]
qubit-batch = "0.3.1"
```

## 快速开始

### 使用 `for_each` 处理数据项

如果你的批处理逻辑是“对每个数据项执行同一个动作”，通常优先使用
`for_each`。它会在内部把闭包转换成可运行任务，并尝试处理每个数据项。

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportError {
    record_id: u64,
    reason: &'static str,
}

let executor = SequentialBatchExecutor::new();

let records = [
    (101, "alice@example.com"),
    (102, "not-an-email"),
    (103, "carol@example.com"),
];

let result = executor
    .for_each(records, 3, |(record_id, email)| {
        if email.contains('@') {
            Ok(())
        } else {
            Err(ImportError {
                record_id,
                reason: "email address is invalid",
            })
        }
    })
    .expect("iterator should yield exactly the declared number of records");

assert_eq!(result.task_count(), 3);
assert_eq!(result.completed_count(), 3);
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);
assert!(!result.is_success());

let failure = &result.failures()[0];
assert_eq!(failure.index(), 1);
match failure.error() {
    BatchTaskError::Failed(error) => {
        assert_eq!(error.record_id, 102);
        assert_eq!(error.reason, "email address is invalid");
    }
    BatchTaskError::Panicked { .. } => unreachable!("the closure returned an error"),
}
```

### 执行显式任务

当任务已经实现 `Runnable`，或者你希望先构造任务对象再统一执行时，可以使用
`execute`。

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};
use qubit_function::Runnable;

#[derive(Debug)]
enum FileTask {
    Validate(&'static str),
    Upload(&'static str),
    Cleanup(&'static str),
}

impl Runnable<&'static str> for FileTask {
    fn run(&mut self) -> Result<(), &'static str> {
        match self {
            Self::Validate(path) if path.ends_with(".csv") => Ok(()),
            Self::Validate(_) => Err("unsupported file type"),
            Self::Upload(_) => Ok(()),
            Self::Cleanup(path) if *path == "/tmp/protected" => {
                panic!("cleanup path is protected");
            }
            Self::Cleanup(_) => Ok(()),
        }
    }
}

let tasks = vec![
    FileTask::Validate("customers.csv"),
    FileTask::Upload("customers.csv"),
    FileTask::Validate("notes.txt"),
    FileTask::Cleanup("/tmp/protected"),
];

let executor = SequentialBatchExecutor::new();
let result = executor
    .execute(tasks, 4)
    .expect("iterator should yield exactly four tasks");

assert_eq!(result.completed_count(), 4);
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);
assert_eq!(result.panicked_count(), 1);

for failure in result.failures() {
    match failure.error() {
        BatchTaskError::Failed(error) => {
            println!("task #{} failed: {error}", failure.index());
        }
        BatchTaskError::Panicked { message } => {
            println!("task #{} panicked: {message:?}", failure.index());
        }
    }
}
```

如果下游 crate 需要自己实现 `Runnable`，也要显式依赖 `qubit-function`：

```toml
[dependencies]
qubit-batch = "0.3.1"
qubit-function = "0.11"
```

### 按固定大小分批处理数据

当真正的处理目标本身支持批处理时，例如 SQL 批量插入，可以为这个目标实现
`BatchProcessor`。如果逻辑批次还需要拆成更小的批次提交，再用
`ChunkedBatchProcessor` 包装这个 processor。

```rust
use std::{
    num::NonZeroUsize,
    time::Duration,
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessor,
    ChunkedBatchProcessor,
};

struct InsertRows;

impl BatchProcessor<i32> for InsertRows {
    type Error = &'static str;

    fn process<I>(&mut self, rows: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let processed = rows.into_iter().count();
        Ok(BatchProcessResult::new(
            count,
            processed,
            processed,
            1,
            Duration::ZERO,
        ))
    }
}

let mut processor = ChunkedBatchProcessor::new(
    InsertRows,
    NonZeroUsize::new(2).expect("chunk size is non-zero"),
);

let result = processor
    .process([1, 2, 3, 4, 5], 5)
    .expect("iterator should yield exactly five items");

assert_eq!(result.completed_count(), 5);
assert_eq!(result.processed_count(), 5);
assert_eq!(result.chunk_count(), 3);
```

## 进度上报

`SequentialBatchExecutor` 默认使用 `NoOpProgressReporter`。你可以挂接自己的
上报器，并调整运行中进度回调的最小间隔。顺序执行器只会在两个任务之间发出
进度回调，因此单个长任务执行期间不会产生中间 running 回调。

```rust
use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
    SequentialBatchExecutor,
};

struct ConsoleReporter;

impl ProgressReporter for ConsoleReporter {
    fn report(&self, event: &ProgressEvent) {
        let counters = event.counters();
        let total = counters.total_count().unwrap_or(counters.completed_count());
        match event.phase() {
            ProgressPhase::Started => println!("starting {total} tasks"),
            ProgressPhase::Running => println!(
                "completed {}/{total}, active {}, elapsed {:?}",
                counters.completed_count(),
                counters.active_count(),
                event.elapsed(),
            ),
            ProgressPhase::Finished => println!("finished {total} tasks in {:?}", event.elapsed()),
            ProgressPhase::Failed | ProgressPhase::Canceled => println!(
                "stopped after {}/{total} tasks in {:?}",
                counters.completed_count(),
                event.elapsed(),
            ),
        }
    }
}

let executor = SequentialBatchExecutor::new()
    .with_reporter(ConsoleReporter)
    .with_report_interval(Duration::from_millis(250));

let result = executor
    .for_each(["a", "b", "c"], 3, |_item| Ok::<(), &'static str>(()))
    .expect("item count should match");

assert!(result.is_success());
```

任务体中的 panic 会被捕获为 `BatchTaskError::Panicked`。但进度上报器本身的
panic 不会被当作任务失败聚合，而是会直接传播给调用者，因为进度上报不属于
任务失败模型。
进度事件只包含 phase、counters、可选 stage 和 elapsed；非进度类可观测性应使用
日志、指标或 tracing。

## 任务数量契约

`execute` 和 `for_each` 都要求调用者传入声明的数据项或任务数量。这样执行器
可以在真正消费迭代器之前上报一致的总数，并能发现生产者错误：

```rust
use qubit_batch::{
    BatchExecutionError,
    BatchExecutor,
    SequentialBatchExecutor,
};

let executor = SequentialBatchExecutor::new();
let error = executor
    .for_each([10, 20], 3, |_value| Ok::<(), &'static str>(()))
    .expect_err("the iterator yielded fewer items than declared");

match error {
    BatchExecutionError::CountShortfall {
        expected,
        actual,
        outcome,
    } => {
        assert_eq!(expected, 3);
        assert_eq!(actual, 2);
        assert_eq!(outcome.completed_count(), 2);
    }
    BatchExecutionError::CountExceeded { .. } => unreachable!(),
}
```

需要特别注意结果语义：

- `Ok(BatchOutcome)` 不代表所有任务都成功，只代表迭代器实际产出数量
  与声明数量一致。
- `result.is_success()` 才是“所有声明任务都完成且没有任务错误或 panic”的便捷
  判断。
- `Err(BatchExecutionError)` 表示迭代器产出的数量少于或多于声明数量；错误中
  仍然携带已经执行部分的 `BatchOutcome`。

## API 说明

- `SequentialBatchExecutor::new()` 是确定性的，会在调用线程中按迭代器顺序执行
  任务。
- `BatchOutcome::failures()` 返回按任务下标排序的失败记录。
- `BatchTaskFailure::index()` 是从 0 开始的下标，对应任务在批次中的位置。
- 核心 crate 刻意避免运行时依赖；如果需要基于 Rayon 的并行执行，可使用配套
  的 `qubit-rayon-batch` crate。

## 公共 API 速览

- `BatchExecutor`：执行一批声明数量的、可能失败的 runnable 任务的 trait。
- `BatchCallResult<R, E>`：callable 批次结果，包含执行汇总和按下标保存的成功返回值。
- `SequentialBatchExecutor`：默认执行器，在调用线程中按顺序执行任务。
- `BatchProcessor`：处理一批声明数量的数据项的 trait。
- `BatchProcessResult`：聚合处理结果，包含数据项数量、已处理数量、chunk 数量和单调耗时。
- `ChunkedBatchProcessor`：把逻辑批次拆成固定大小 chunk，并逐个委托给代理 processor
  的包装器。
- `ChunkedBatchProcessError<E>`：chunked processor 的错误类型，用于表示输入数量不匹配
  或代理 processor 失败，并携带部分处理结果。
- `ProgressReporter`：接收 `ProgressEvent` 的 trait，用于批次开始、周期性进度和
  终态通知。
- `NoOpProgressReporter`：默认进度上报器，接收回调但不执行任何操作。
- `WriterProgressReporter` 和 `LoggerProgressReporter`：
  分别向 writer、stdout 和 `log` crate 输出进度信息的具体上报器。
- `BatchOutcome<E>`：聚合执行结果，包含任务计数、单调耗时和详细失败记录。
- `BatchExecutionError<E>`：批次级契约错误，用于表示声明数量不足或超出，并携带
  已收集到的部分结果。
- `BatchTaskFailure<E>`：单个失败或 panic 任务的记录，包含稳定的从 0 开始的批次下标。
- `BatchTaskError<E>`：任务级错误，区分任务返回的业务错误和被捕获的 panic。

## 项目结构

- `src/executor`：执行器 trait 与顺序执行器实现。
- `src/error`：批处理执行结果、数量不匹配错误、任务失败记录和 panic 转换。
- `src/processor`：数据项批处理 trait、结果类型和 chunked processor。
- `src/progress`：从 `qubit-progress` 重导出的进度事件和上报器类型。
- `tests/executor`：顺序执行、进度回调、失败、panic 和数量不匹配的行为测试。
- `tests/processor`：chunked processor 的分块、错误和进度行为测试。
- `tests/progress`：具体进度上报器的行为测试。
- `tests/error`：结果不变量和错误辅助方法测试。
- `tests/docs`：README 一致性测试。

## 文档

- API 文档：[docs.rs/qubit-batch](https://docs.rs/qubit-batch)
- Crate 发布页：[crates.io/crates/qubit-batch](https://crates.io/crates/qubit-batch)
- 源码仓库：[github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)

## 测试与 CI

在 crate 根目录快速执行本地检查：

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

若要与仓库 CI 环境保持一致，请运行：

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

`./align-ci.sh` 会先对齐本地工具链和 CI 相关配置；`./ci-check.sh` 复现流水线检查。
修改运行期行为并需要关注覆盖率时，可配合使用 `./coverage.sh`。

## 参与贡献

欢迎通过 Issue 与 Pull Request 参与本仓库。建议单次变更聚焦一个主题；修改行为时
补充或更新测试；影响公开 API 或用户可见行为时，同步更新本文档或 rustdoc。

向本仓库贡献内容即表示您同意以 [Apache License, Version 2.0](LICENSE)（与本项目相同）
授权您的贡献。

## 许可证与版权

版权所有 © 2026 Haixing Hu，Qubit Co. Ltd.。

本软件依据 [Apache License, Version 2.0](LICENSE) 授权；完整许可文本见仓库根目录的
`LICENSE` 文件。

## 作者与维护

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **源码仓库** | [github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch) |
| **API 文档** | [docs.rs/qubit-batch](https://docs.rs/qubit-batch) |
| **Crate 发布** | [crates.io/crates/qubit-batch](https://crates.io/crates/qubit-batch) |
