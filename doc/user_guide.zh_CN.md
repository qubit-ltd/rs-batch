# Qubit Batch 用户手册

[English user guide](user_guide.md) · [README](../README.zh_CN.md) ·
[API 文档](https://docs.rs/qubit-batch)

本文适用于 `qubit-batch` 0.10 和 Rust 1.94 及以上版本。面向需要立刻处理一批有限
数据、并希望拿到可审计结果的应用或库作者；它不用于构建常驻队列、调度器或 worker pool。

## 手册目标与读者

当每个输入项都是一个可独立失败的任务，并且你需要任务失败、panic、耗时和稳定下标时，
使用 `BatchExecutor`。当有状态组件要直接消费数据项，并报告完成或处理数量时，使用
`BatchProcessor`。本 crate 只消费一次输入来源，不提供重试、持久化存储或特定异步运行时。

## 概念模型

```text
有限输入来源 ──> executor 或 processor ──> outcome/result
                  │                           ├─ 计数与耗时
                  │                           ├─ 带下标的任务失败记录
                  │                           └─ 批次错误附带的部分结果
                  └─ 声明数量契约
```

`for_each` 等 API 会从 `ExactSizeIterator` 推导声明数量；`*_with_count` 形式适合
惰性来源或由外部系统统计数量的场景。任务返回 `Err` 时，结果仍是正常的
`BatchOutcome`；来源数量不匹配或进度上报失败时，返回 `BatchExecutionError`，其中保留
已经统计到的部分结果。

## 贯穿场景：校验导入批次

导入服务需要校验全部记录、汇总所有无效记录，并按原始位置安排重试。完成标准是：尝试
3 条记录，成功 2 条，失败记录的下标为 1。

### 安装 crate

```toml
[dependencies]
qubit-batch = "0.10"
```

### 执行全部校验任务

```rust
use qubit_batch::{
    BatchExecutor,
    BatchTaskError,
    SequentialBatchExecutor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportError {
    record_id: u64,
}

let records = [
    (101, "alice@example.com"),
    (102, "not-an-email"),
    (103, "carol@example.com"),
];

let outcome = SequentialBatchExecutor::new()
    .for_each(records, |(record_id, email)| {
        if email.contains('@') {
            Ok(())
        } else {
            Err(ImportError { record_id })
        }
    })
    .expect("array length should be exact");

assert_eq!(outcome.task_count(), 3);
assert_eq!(outcome.succeeded_count(), 2);
assert_eq!(outcome.failed_count(), 1);
assert_eq!(outcome.failures()[0].index(), 1);
assert!(matches!(outcome.failures()[0].error(), BatchTaskError::Failed(_)));
```

`SequentialBatchExecutor` 在调用线程中按迭代器顺序执行。默认策略会继续处理任务错误
和已捕获的任务 panic，因此可从 `outcome.failures()` 生成完整的重试报告。

## 核心工作流

根据已有输入选择入口：

- `for_each` 把数据项和闭包转换成任务。
- `execute` 接收 `qubit-function` 的 `Runnable` 任务。
- `call` 接收 `Callable` 任务，并在 `BatchCallResult` 中按原始下标保存成功返回值。
- `process` 将数据项直接交给 `BatchProcessor` 实现。

输入实现 `ExactSizeIterator` 时，应优先使用这些默认方法；它们会在消费来源时校验数量。
当数量由数据库查询或其他外部边界提供时，再使用对应的 `*_with_count` 方法，并把数量
视为该边界的一部分契约。

### Callable 与 processor 契约

`SequentialBatchExecutor` 的具体 callable 方法通过 `FnMut` 行为接收 callable 值。
会修改捕获状态的闭包不需要实现 `Fn`。`BatchExecutor` trait 仍为并行实现保留 `Send`
约束，因为已接受的任务和返回值可能跨越 scoped worker 线程。

对于 `BatchProcessor`，`processed_count` 统计成功处理的输入项数量，并且必须满足
`processed_count <= completed_count <= item_count`。数据库受影响行数等业务指标属于
processor 自己维护的独立状态；当它可能超过输入数量时，不能写入 `processed_count`。

### 显式分块处理较大来源

当单个结果不适合承载完整来源时，可以使用已有 callable API。下面每次循环拥有独立的
outcome，是否在失败后继续由外层循环决定：

```rust
use qubit_batch::{BatchExecutor, SequentialBatchExecutor};

let executor = SequentialBatchExecutor::new();
let mut source = 0..10_000usize;
let mut offset = 0usize;
let mut sum = 0usize;

loop {
    let chunk: Vec<_> = source.by_ref().take(256).collect();
    if chunk.is_empty() {
        break;
    }
    let chunk_len = chunk.len();
    let result = executor
        .call(chunk.into_iter().map(|item| move || Ok::<_, ()>(item)))
        .expect("chunk length should be exact");
    assert!(result.outcome().is_success());
    for output in result.into_outputs() {
        let global_index = offset + output.index();
        let _ = global_index;
        sum += *output.value();
    }
    offset += chunk_len;
}

assert_eq!(sum, (0..10_000usize).sum());
```

这种模式不提供全局 failure policy、全局稳定下标或跨 chunk 自动重试。Callable 下标
只在各自的结果内有效；上例中的 `offset` 是调用方映射全局下标的方式。失败 chunk 的
错误只会暴露失败之前已成功完成的 chunk 的结果；是否重试失败边界以及如何保证幂等性，
都由调用方明确决定。

## 进阶用法

### 有意识地使用 scoped 并行 worker

`ParallelBatchExecutor` 使用固定宽度的 scoped 标准线程。任务可以借用调用方数据，
方法返回前所有任务都会结束。默认配置会把声明数量不超过 100 的批次顺序执行，以避免
创建 worker 的成本。只有在真实负载测量后再通过 builder 调整配置：

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

let outcome = executor
    .for_each(0..8, |value| {
        assert!(value < 8);
        Ok::<(), &'static str>(())
    })
    .expect("range length should be exact");

assert!(outcome.is_success());
```

`sequential_threshold(0)` 表示所有非空批次都使用 scoped worker；它不会创建可复用
线程池。若需要 Rayon 支持，请使用配套的 `qubit-rayon-batch` crate。该 crate 在同一个
线程池中嵌套调用时，会在发起嵌套调用的 worker 上回退到顺序执行，避免等待同一线程池的
worker；任意任务依赖或跨池循环等待仍需由应用设计处理。

### 在任务失败后提前停止

默认的 `TaskFailurePolicy::Continue` 会收集全部任务失败。若达到阈值后不应继续接受
来源中的数据项，可通过 executor builder 配置 `StopOnFirstFailure` 或
`StopAfterFailures(...)`。此时即使得到 `Ok(BatchOutcome)`，也可能是提前停止；在把
来源数量视为已完整校验前，应先检查 `outcome.termination()`。

运行时相关的调度器拥有惰性来源时，应使用 `next_task`，这样可以把来源返回
`None` 与准入停止分别记录：

```rust
use std::{convert::Infallible, sync::Arc, time::Duration};
use qubit_batch::{BatchExecutor, TaskFailurePolicy};
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_progress::NoopReporter;

let coordinator = ParallelBatchExecutionCoordinator::new(
    Arc::new(NoopReporter), Duration::ZERO);
let tasks = [|| Ok::<(), &'static str>(())];
let outcome = coordinator.execute(tasks, 1, TaskFailurePolicy::Continue,
    |tasks, context| {
        let mut source = tasks.into_iter();
        while let Some(token) = context.next_task(&mut source) {
            context.execute_task(token);
        }
        Ok::<(), Infallible>(())
    }).unwrap();
assert!(outcome.is_success());
```

## 错误与诊断

应分两层检查结果：

1. 对 `Ok(BatchOutcome)`，检查 `is_success()`、各项计数和 `failures()`。任务返回错误
   会显示为 `BatchTaskError::Failed`，任务 panic 会显示为 `BatchTaskError::Panicked`。
2. 对 `Err(BatchExecutionError)`，先检查 `outcome()`，再处理错误。`CountShortfall` 和
   `CountExceeded` 会保留声明数量和实际观察数量；进度上报失败与未完成调度也会保留已
   统计到的工作结果。

对于 `call`，`BatchCallError` 还会保存批次级错误发生前已经成功的、稀疏的
`BatchCallOutput`。成功的 `BatchCallResult` 保留 S 个成功值和 F 条有序失败记录，因此
校验和保留结果的空间复杂度为 O(S + F)。不要把单项任务失败直接当成重试整个批次的信号：
本 crate 并不知道任务副作用是否幂等。

对于 `ChunkedBatchProcessor`，错误附带的 result 只包含失败 chunk 之前已成功完成的
chunk。失败 chunk 可能已经产生外部副作用，因此该 chunk 是重试边界；是否以及如何重试，
必须由调用方决定。

## 排障

| 现象 | 检查项 | 处理方式 |
| --- | --- | --- |
| 返回 `Ok` 但仍有失败 | `outcome.is_success()` 与 `failures()` | 按下标处理任务错误或 panic。 |
| 出现 `CountShortfall` | 外部声明数量和输入生产者 | 修正生产者或数量边界；通过 `outcome()` 获取已完成工作。 |
| 小批次没有启用 worker | `sequential_threshold()` | 仅在基准测试后降低阈值。 |
| panic 没有作为错误返回 | panic 是否发生在任务体中 | 任务体 panic 会被捕获；进度上报器和 processor consumer 的 panic 可能直接传播。 |

## 限制与最佳实践

- 输入来源必须是有限的；本 crate 不承担持久化调度、重试编排或存储职责。
- 把声明数量视为契约；只有数量确实来自独立来源时才使用 `*_with_count`。
- 每次并行调用都会创建 scoped worker，并在返回前等待已接受任务；线程数和阈值应以
  实际测量为依据。
- 进度间隔只是在实现定义的进度点做节流，不保证时间一到立即产生 running 事件。
- 重试策略应在 crate 外部实现，并针对任务副作用保证安全性。

## 延伸阅读

- [README](../README.zh_CN.md)
- [English user guide](user_guide.md)
- [API 文档](https://docs.rs/qubit-batch)
- [Crate 发布页](https://crates.io/crates/qubit-batch)
