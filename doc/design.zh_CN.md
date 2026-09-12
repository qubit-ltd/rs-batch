# 批处理执行设计

[English design](design.md) · [用户手册](user_guide.zh_CN.md) · [README](../README.zh_CN.md)

本文描述 qubit-batch 0.12 的架构与契约，最低工具链仍为 Rust 1.94、edition 2024。
标准线程执行器与配套 qubit-rayon-batch 共同遵守下述运行与结果约束。

## 职责与所有权

Executor 处理相互独立、可失败的任务；processor 表示直接消费一批数据的组件，
例如把数据块交给数据库。失败块可能已经产生部分外部副作用，因此两者保留各自的
错误和结果类型。重试、事务、持久化队列和异步运行时均由应用负责。

顺序执行器在调用线程运行；标准并行执行器每次调用创建 scoped worker；Rayon
使用专有、可复用的线程池。两种并行后端都将进度生命周期、任务统计与最终校验
交给 ParallelBatchExecutionCoordinator。调度闭包负责消费来源、有界派发与等待任务。

私有状态与任务适配器分别位于 execute/internal 和 process/internal，处理器的
带下标任务封装位于 utils/internal。测试不通过新增公共接口访问实现。
ProgressFailure::fail_operation 仅共享失败终止事件的投递与错误转换；各结果
所有者仍负责构建部分结果和确定主要错误。

## 准入与完成

```text
来源 next() → 已观察 → 已接受 token → 已开始 → 成功 / 失败
                 │          │                      │
               超限拒绝    返回前排空             计数与失败明细
```

Token 带有本次执行的身份和唯一来源下标，外部不能直接构造或克隆。
execute_task 消耗 token，并拒绝来自其他执行上下文的 token。调度器返回成功后，
coordinator 会检查所有已接受 token 是否都已完成。

调度器优先使用 next_task。来源返回 None 会记录耗尽；准入返回 None 也可能是
失败策略停止、进度失败或声明数量超限。最终原因以 coordinator 的结果为准。
accept_task 是较低层的适配入口，不替调用方记录来源耗尽。

失败策略采用协作式停止：已经接受的任务仍会完成，所以最终失败次数可以超过
配置阈值。准入可能与 worker 失败同时发生，不保证最后接受的精确下标。
拉取前检查能避免在已经观察到停止后再次拉取，但不能取消正在运行的 next 调用。

只有真正观察到来源 None 才证明耗尽。如果先观察到耗尽，之后 worker 才失败，
数量不足仍报告 CountShortfall，不能被策略停止覆盖。Ok outcome 中的 Finished
表示没有因策略短路，不代表所有任务成功；错误携带的 outcome 只是部分统计，
还必须检查外层错误。

## 错误优先级与进度

启动上报失败时不进入调度。调度返回后，按以下顺序处理：

1. 调度拒绝，同时保留自动上报或终止上报的次要错误。
2. 自动上报失败，同时保留已经完成的任务统计。
3. 已接受却未完成的 token：IncompleteSchedule。
4. 已观察的数量超限：CountExceeded。
5. 尚未观察到耗尽时的失败策略停止。
6. 来源耗尽或调度返回后发现数量不足：CountShortfall。
7. 要求完整消费的来源未观察到 `None`：IncompleteSchedule；失败终态投递错误保留在
   `report_error` 中。
8. 终止进度投递与最终 outcome。

同步进度回调、迭代器和任务析构的 panic 可能传播到调用方。任务体的 unwind
会被捕获，但 abort 模式的 panic 无法捕获。自动上报的错误与捕获到的 panic
转换为 ProgressFailure。Processor consumer 不使用 executor 的任务 panic 捕获语义。

正数进度间隔用于节流，不是定时投递保证。顺序路径在数据项之间报告，分块路径在
成功块之间报告；并行路径使用 scoped reporter 线程。零间隔取消节流，通过完成
通知触发报告而不忙等；多个通知可能合并。回调不得与 worker 或等待批次结束的
线程形成循环依赖。

## 结果与分块边界

BatchOutcome 满足 completed = succeeded + failed + panicked，且 completed 不超过
声明 task_count；每个失败或 panic 对应一个唯一、范围内的下标，失败明细按下标排序。
BatchCallResult 的成功输出稀疏保存、下标严格递增、与失败下标互斥，数量等于
succeeded_count。交叉校验的时间复杂度为 O(S + F)，输出收集还可能需要单独排序。

BatchCallError 只保留本次调用的成功输出。应用多次调用 call 时，应自行保存此前
结果并维护全局下标偏移。单项任务失败仍可返回 Ok(BatchCallResult)。

BatchProcessResult 满足 processed <= completed <= item_count。Processed 统计成功
处理的输入项，不能用于记录受影响数据库行数。ChunkedBatchProcessor 要求成功
delegate 结果的 item_count 与 completed_count 等于提交块长度，但允许 processed
更低。Delegate 失败或返回不合法成功结果时，外层只累计此前成功块；当前块可能
已经产生副作用，delegate 自身的错误可以携带更详细的业务状态。

## 校验与 0.10 迁移

Outcome 校验不再分配 HashSet 保存下标：先检查聚合计数与越界，再原地排序、
检查相邻重复，最后检查失败明细类型。F 条失败记录的排序最坏时间为 O(F log F)，
不再分配额外堆缓冲区。

多个非法条件同时存在时，错误选择有意改变：越界优先于重复，重复错误报告最小
重复下标；多个越界项仍报告原输入顺序中的第一个。聚合计数错误与 callable 结果
错误的优先级保持不变。原地校验与错误优先级变化最早随 qubit-batch 0.11 引入；
当前 0.12 沿用该行为。当前配套版本为 qubit-rayon-batch 0.10，依赖 qubit-batch 0.12。

## 资源与验证

标准 worker 及有界队列随线程数 W 增长；当前实现的未完成来源观察数上限为
2W + 1。这是后端测试覆盖的实现属性，不是任意自定义调度器的统一保证。
失败明细占 O(F) 空间，callable 结果占 O(S + F)。应用显式分块可以限制输出保留量，
但不提供跨块全局策略。默认顺序阈值保持 100，应根据真实负载测量后调整。

公开行为在根 tests 验证。真实准入 Loom 模型位于 src/tests，运行时同时启用
RUSTFLAGS=--cfg loom 和 loom-tests feature。独立布尔原子模型已经移除。
Rayon 仓库用共享泛型断言覆盖顺序、标准并行、Rayon 及回退路径；后端专有测试
继续验证准入窗口、任务排空和重入。

两份 README 与两份指南直接作为 doctest 编译。基准的正确性断言放在计时区外；
端到端并发基准包含调用线程创建成本，steady-callers 基准复用调用线程，但计入
请求与结果通道交接。性能结论必须说明负载与环境，不承诺普遍加速。

[性能测量](performance.zh_CN.md)

## Callable 回退与分块存储

`execute::spi::call_with_executor` 统一处理稀疏输出，调用 `execute_with_count`，不递归调用
后端覆盖的 callable 方法。外层惰性迭代器把用户 `into_iter` 延迟到执行器开始准入之后。
标准线程与 Rayon 的 callable 回退直接使用顺序 Vec 收集；Rayon 在选择并行路径后先释放
临时 guard，再由 helper 进入 `execute_with_count` 的保护范围，这段间隔不执行用户代码。
`BatchCallError::map_scheduler_error` 保留成功值及次级 reporter 错误，不要求 Clone。

分块委托改用 `Vec::drain(..)`，缓冲区容量由外层保留。delegate 未消费完时，Drain 的析构
负责释放剩余元素。delegate 失败或返回非法成功结果时，汇总仍不计入当前块；不新增来源
消费审计，也不保证具体析构顺序。

策略终止标签反映来源是否被观察为耗尽，并非完成数量。最后一个任务失败时可能已完成全部
声明任务，但尚未观察到 None；并行路径也可能先耗尽来源再失败。测试用通道握手分别验证
这两种时序。
