# Qubit Batch

[![Rust CI](https://github.com/qubit-ltd/rs-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-batch/coverage-badge.json)](https://qubit-ltd.github.io/rs-batch/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-batch.svg?color=blue)](https://crates.io/crates/qubit-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-batch` 用于一次性执行有限批次，并返回结构化统计结果。它适合数据校验、
导入和运维等场景：调用方既要处理一组相互独立的数据，又不希望公共库绑定到某个
异步运行时。

## 安装

```toml
[dependencies]
qubit-batch = "0.11"
```

需要 Rust 1.94 或更高版本。只有直接实现 `Runnable`、`Callable` 或 `Consumer`
时才需加入 `qubit-function`；只有实现自定义进度上报器时才需加入 `qubit-progress`。

## 快速开始

一个导入服务拿到三条记录，希望所有记录都经过校验，并在重试报告中保留无效记录的
稳定下标。

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

数组长度与实际来源一致，因此调用正常返回；单项失败保留在 `BatchOutcome` 中供调用方
检查。

## 为什么需要它

许多应用只需要完成一次有边界的批量操作，并不需要队列或常驻 worker pool。
本 crate 统一处理数量校验、耗时统计、稳定失败下标、任务 panic 捕获和部分结果，
把重试策略与运行时选择留给应用自身。

## 核心能力与边界

- `BatchExecutor` 用于可失败任务，`BatchProcessor` 用于直接处理数据项。
- `SequentialBatchExecutor` 和 `ParallelBatchExecutor` 分别用于调用线程执行和
  scoped 并行任务执行。
- 支持调用线程上的顺序执行，以及面向较大批次的固定宽度 scoped 标准线程。
- `BatchOutcome` 和 `BatchProcessResult` 提供计数、带下标的失败记录；批次级错误也会
  携带部分结果。
- 可通过 `*_with_count` API 声明数量契约，并配置任务失败后的停止策略。

它不是队列、调度器、持久化 worker pool、重试框架或 Rayon 适配器。基于 Rayon 的
执行能力由配套 crate `qubit-rayon-batch` 提供。并行 worker 的生命周期仅覆盖一次调用；
修改默认顺序回退阈值前，应先基于真实负载进行基准测试。

## 延伸阅读

- [User guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [Design](doc/design.md)
- [中文设计说明](doc/design.zh_CN.md)
- [API 文档](https://docs.rs/qubit-batch)
- [Crate 发布页](https://crates.io/crates/qubit-batch)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)
