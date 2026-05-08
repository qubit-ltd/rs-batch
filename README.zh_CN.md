# Qubit Batch

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-batch.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-batch)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-batch/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-batch?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Documentation](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的一次性批量执行与批量处理工具 crate。

## 功能定位

当你已经有一个有限批次，并希望执行一次后获得一致的统计、失败定位和部分结果时，
可以使用 `qubit-batch`：

- 数据导入、数据校验或运维任务中，希望每条记录都被尝试处理；
- 需要稳定的从 0 开始的失败下标，用于诊断和重试；
- 需要汇总完成、成功、失败和 panic 的任务数量；
- 需要发现迭代器实际产出数量少于或多于声明数量的生产者错误；
- 公共库代码不希望绑定 Tokio、Rayon 或其他具体运行时。

这个 crate 不是队列、调度器、工作线程池或重试框架。它只消费调用者提供的
迭代器一次，并返回结构化结果。

## 核心模型

- `BatchExecutor` 执行可能失败的任务。数据项批处理优先使用 `for_each`，已有
  `Runnable` 任务时使用 `execute`，需要返回值的 `Callable` 任务使用 `call`。
- `BatchOutcome` 是执行结果，包含任务计数、耗时和带下标的 `BatchTaskFailure`。
- `BatchExecutionError` 是批次契约错误，表示迭代器产出数量与声明数量不匹配，
  并携带部分 `BatchOutcome`。
- `SequentialBatchExecutor` 在调用线程中按迭代器顺序执行任务。
- `ParallelBatchExecutor` 使用固定宽度的 scoped 标准线程执行任务。
- `BatchProcessor` 直接处理数据项，不要求先把数据项包装为任务。
- `SequentialBatchProcessor` 和 `ParallelBatchProcessor` 对每个数据项调用一个
  `qubit-function` `Consumer`，并支持进度上报。
- `ChunkedBatchProcessor` 把一个逻辑批次拆成固定大小的 chunk，并把每个 chunk
  委托给另一个 `BatchProcessor`。delegate 对某个 chunk 返回 `Ok` 时，必须报告
  `item_count == chunk_len` 且 `completed_count == chunk_len`；当底层操作报告
  的成功数或影响行数更少时，`processed_count` 可以小于 chunk 长度。

基于 Rayon 的批量执行器位于配套的 `qubit-rayon-batch` crate。

## 安装

```toml
[dependencies]
qubit-batch = "0.5.0"
```

当你要直接实现 `Runnable`、`Callable` 或 `Consumer` 类型时，需要额外依赖
`qubit-function`。当你要实现自定义进度上报器时，需要额外依赖 `qubit-progress`。

## 示例

### 校验每个数据项

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
assert_eq!(result.succeeded_count(), 2);
assert_eq!(result.failed_count(), 1);

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

### 并行执行

```rust
use qubit_batch::{
    BatchExecutor,
    ParallelBatchExecutor,
};

let executor = ParallelBatchExecutor::builder()
    .thread_count(4)
    .sequential_threshold(0)
    .build()
    .expect("parallel executor configuration should be valid");

let result = executor
    .for_each(0..8, 8, |value| {
        assert!(value < 8);
        Ok::<(), &'static str>(())
    })
    .expect("range length should match the declared count");

assert!(result.is_success());
```

`ParallelBatchExecutor::default()` 会把声明任务数不超过 100 的批次交给顺序执行器，
以避免 scoped 线程创建成本。需要所有非空批次都走并行 worker 时，可设置
`sequential_threshold(0)`。

### 收集 callable 返回值

```rust
use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};

fn count_users() -> Result<usize, &'static str> {
    Ok(3)
}
fn count_orders() -> Result<usize, &'static str> {
    Ok(5)
}

let result = SequentialBatchExecutor::new()
    .call([count_users, count_orders], 2)
    .expect("callable count should match");

assert!(result.outcome().is_success());
assert_eq!(result.values(), &[Some(3), Some(5)]);
```

### 直接处理数据项

```rust
use qubit_batch::{
    BatchProcessor,
    SequentialBatchProcessor,
};

let mut processor = SequentialBatchProcessor::new(|item: &i32| {
    assert!(*item > 0);
});

let result = processor
    .process([1, 2, 3], 3)
    .expect("iterator should yield exactly three items");

assert_eq!(result.completed_count(), 3);
assert_eq!(result.processed_count(), 3);
```

### 委托固定大小 chunk

```rust
use std::{
    num::NonZeroUsize,
    time::Duration,
};

use qubit_batch::{
    BatchProcessResult,
    BatchProcessResultBuilder,
    BatchProcessor,
    ChunkedBatchProcessor,
};

struct InsertChunk;

impl BatchProcessor<i32> for InsertChunk {
    type Error = &'static str;

    fn process<I>(&mut self, rows: I, count: usize) -> Result<BatchProcessResult, Self::Error>
    where
        I: IntoIterator<Item = i32>,
    {
        let processed = rows.into_iter().count();
        BatchProcessResultBuilder::builder(count)
            .completed_count(processed)
            .processed_count(processed)
            .chunk_count(1)
            .elapsed(Duration::ZERO)
            .build()
            .map_err(|_| "invalid process result")
    }
}

let mut processor = ChunkedBatchProcessor::new(
    InsertChunk,
    NonZeroUsize::new(2).expect("chunk size is non-zero"),
);

let result = processor
    .process([1, 2, 3, 4, 5], 5)
    .expect("iterator should yield exactly five items");

assert_eq!(result.completed_count(), 5);
assert_eq!(result.processed_count(), 5);
assert_eq!(result.chunk_count(), 3);
```

`ChunkedBatchProcessor` 委托一个 chunk 时，会把 delegate 返回的结果视为这个
已提交 chunk 的结果。返回 `Ok` 表示 delegate 已经让 chunk 内每个数据项都达到
终态，因此 `item_count` 和 `completed_count` 必须都等于提交的 chunk 长度。
`processed_count` 可以小于 chunk 长度，用于表达目标系统报告的成功数更少，例如
幂等数据库插入接受了 3 行但实际只影响 2 行。如果 delegate 无法让整个 chunk
达到终态，应返回 `Err`；不一致的 `Ok` 结果会被报告为
`ChunkedBatchProcessError::InvalidChunkResult`。

## 进度上报

`qubit-batch` 接受 `qubit-progress` 的上报器，但不重新导出 `qubit-progress`
中的类型。自定义上报器应直接实现 `qubit-progress` 的 trait。
`SequentialBatchExecutor`、`ParallelBatchExecutor`、`SequentialBatchProcessor`、
`ParallelBatchProcessor` 和 `ChunkedBatchProcessor` 都可以挂接自定义上报器。

```rust
use std::time::Duration;

use qubit_batch::{
    BatchExecutor,
    SequentialBatchExecutor,
};
use qubit_progress::{
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
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

任务体中的 panic 会被捕获为 `BatchTaskError::Panicked`。processor consumer 和
进度上报器本身的 panic 会直接传播给调用者，因为它们不属于任务失败模型。
顺序执行和顺序处理只会在两个任务或数据项之间上报进度；并行变体通过
`Progress::spawn_running_reporter` 在 scoped 上报线程中周期性发送 running 进度。

配置的 `report_interval` 是在实现代码到达 running 进度点时检查的节流条件，
不保证时间一到就立刻发出 running 事件。顺序变体在任务或数据项之间检查，
chunked processing 在一个 chunk 完成后检查。并行变体使用 scoped 上报线程；
当 interval 大于 0 时，也可以在 worker 活跃期间周期性发送 running 事件。
`Duration::ZERO` 表示关闭时间节流：每当实现代码到达自己的 running 进度点时
就尽快上报，但不会因此进入持续刷新循环。

## 任务数量契约

执行和处理 API 都要求调用者传入声明数量。这样 API 可以在消费惰性迭代器前
获得稳定总数，并在生产者产出数量不正确时返回部分结果。

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
- `result.is_success()` 表示所有声明任务都完成，并且没有任务错误或 panic。
- `Err(BatchExecutionError)` 表示迭代器产出数量少于或多于声明数量，并携带部分
  `BatchOutcome`。

## API 速览

- `SequentialBatchExecutor::new()` 在调用线程中按迭代器顺序确定性执行任务。
- `ParallelBatchExecutor::default()` 使用可用 CPU 并行度、scoped 标准线程，并对
  声明任务数不超过 100 的批次使用顺序回退。使用
  `ParallelBatchExecutor::builder().sequential_threshold(0)` 可让所有非空批次都走
  并行 worker。
- `BatchOutcome::failures()` 返回按从 0 开始的任务下标排序的失败记录。
- `BatchCallResult::values()` 只为成功 callable 保存 `Some(value)`；失败或 panic
  的 callable 位置为 `None`。
- `BatchProcessResult::processed_count()` 是代理 processor 报告的成功数量。对于
  受影响行数等目标侧计数，它可能与 `completed_count()` 不同。
- `ChunkedBatchProcessError<E>` 在数量不匹配和代理失败时携带部分聚合结果。

## 项目结构

- `src/execute`：批量执行 trait、执行结果、数量不匹配错误、任务失败记录和
  执行适配器。
- `src/execute/impls`：基于标准库的批量执行器实现。
- `src/process`：数据项批处理 trait、结果类型和处理错误。
- `src/process/impls`：consumer-backed processor 和 chunked processor。
- `src/utils`：执行和处理模块共享的 crate 内部工具。
- `tests/execute`：批量执行、进度回调、失败、panic、结果和数量不匹配的行为测试。
- `tests/process`：direct processor、chunked processor、错误和进度行为测试。
- `tests/utils`：共享内部工具行为测试。
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
