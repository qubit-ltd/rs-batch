# 性能测量

[English](performance.md) · [设计说明](design.zh_CN.md)

## 失败索引校验

测量日期为 2026-09-07，使用 Rust 1.94.0、release profile、Linux x86_64
和六核 Intel Core i5-9600K。基线为提交 `2067172`（qubit-batch 0.10.0），
候选版本为 0.11.0 的原地排序实现。输入包含 F 个失败任务，索引唯一且逆序。
输入向量的构造和销毁不计入 build 测量区间。单线程进程包装系统分配器，
统计分配和重新分配请求，在调用 `build()` 前立即重置计数。

每个规模重复三次，结果完全一致：

| F | 基线分配次数 | 基线请求字节数 | 候选分配次数 | 候选请求字节数 |
|---:|---:|---:|---:|---:|
| 1,000 | 1 | 18,448 | 0 | 0 |
| 10,000 | 1 | 147,472 | 0 | 0 |
| 100,000 | 1 | 1,179,664 | 0 | 0 |

这些数值表示校验过程中的额外分配请求，不是结果总内存或进程驻留内存。
结果仍持有 O(F) 的失败向量。可重复的分配减少是移除辅助 HashSet 的依据。
错误优先级变化见[迁移说明](design.zh_CN.md)。

Criterion 耗时没有得到确定结论：100,000 条失败记录的基线点估计为
2.0654 ms，候选两次为 0.3395 ms 和 3.63 ms。未受控环境不能证明可重复的
加速，因此不能据此选择并行阈值或宣称吞吐量提升。

## 复现

建立 edition 2024 的临时 Cargo 二进制项目，将依赖分别命名为 `old` 和
`new`，均设置 `package = "qubit-batch"`，路径指向基线和候选 checkout。
将两者放在同一个 rs-progress checkout 旁，使相对路径依赖解析为同一包。
执行 `cargo run --release`。完整探针源码见[英文版的复现章节](performance.md#reproduction)，
两种语言共用该源码。探针不创建工作线程，避免其它分配与计数重置竞争。

## 调度工作负载

`cargo bench --bench scheduling_matrix_bench` 比较顺序执行器和 scoped
执行器。普通组测量单次调用；`end-to-end-concurrent-*` 包含调用线程的
创建和 join；`steady-callers-*` 在计时前创建并复用调用线程，但计入请求与
结果通道的交接开销。两者均在计时前验证正确性。标准执行器自身的工作线程
创建仍计入所有组的每次批次调用。使用 `cargo bench --bench
scheduling_matrix_bench -- --test` 运行正确性冒烟检查。这些工作负载不测量
Rayon；其基准位于 rs-rayon-batch。
