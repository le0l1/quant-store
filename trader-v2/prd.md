**框架名称:** (待定，实现时可以命名，暂称为) AsyncEventTrader

**核心设计原则:**

1.  **事件驱动 (Event-Driven):** 系统中的所有活动和状态变化都表示为事件。组件通过发布和订阅事件进行通信，实现高度解耦。
2.  **Asyncio 并发 (Asyncio Concurrency):** 利用 Python 的 `asyncio` 库实现基于协程的并发，特别适用于处理高并发的 I/O 操作（网络通信、文件读写），保持系统非阻塞和高响应性。
3.  **模块化与可插拔性 (Modularity & Pluggability):** 系统被划分为具有明确职责的独立模块。核心功能（如数据源、执行器）通过接口抽象，允许在不修改核心逻辑的情况下替换具体实现，以支持回测和实盘。
4.  **性能保障 (Performance Assurance):** 识别并解决潜在的阻塞点，确保事件循环在高吞吐量下依然流畅运行。

**主要架构组件:**

1.  **Event Bus (事件总线):**
    * **职责:** 中央消息分发中心，接收来自各组件的事件，并将事件路由到所有订阅了该事件类型的 Handler。
    * **实现:** 基于 `asyncio.Queue` 实现事件队列，由一个或多个异步消费者任务从队列中获取事件并调用相应的异步处理协程 (`async def`)。
    * **关键功能:** `subscribe(event_type, handler_coroutine)`, `publish(event)`。

2.  **Data Feed (数据源):**
    * **职责:** 负责获取原始市场数据（历史数据或实时数据），并将其转化为标准化的 `MarketEvent` 发布到 Event Bus。
    * **实现:**
        * `BacktestDataFeed`: 从历史数据源（文件、数据库）按时间顺序读取数据，并以**时间步进的方式**发布 `MarketEvent(T)`。在发布每个时间步的数据后，**会异步等待 (await)** Event Bus 队列变空，确保当前时间步的事件处理链基本完成，再推进到下一个时间步 $T+1$。
        * `LiveDataFeed`: 连接到实时市场数据源（如 WebSocket API），接收数据后立即创建并发布 `MarketEvent`。它是**持续运行且非等待的**。

3.  **Strategy (策略):**
    * **职责:** 包含核心交易逻辑。接收 `MarketEvent` 和其他相关事件，分析市场状态，生成交易信号。
    * **订阅事件:** `MarketEvent`，可能订阅 `FillEvent` 等。
    * **发布事件:** `SignalEvent` (包含交易意图)。
    * **实现注意事项:** 其事件处理协程可能包含计算密集型任务。必须确保这些任务通过 `asyncio.get_running_loop().run_in_executor()` 进行异步化处理，避免阻塞事件循环。

4.  **Portfolio (投资组合):**
    * **职责:** 管理账户资金、持仓头寸、风险敞口。接收 `SignalEvent`，根据当前投资组合状态决定是否生成具体的交易订单，并根据 `FillEvent` 更新持仓和资金。
    * **订阅事件:** `SignalEvent`, `FillEvent`，可能订阅 `MarketEvent` 用于估值或条件判断。
    * **发布事件:** `OrderEvent` (具体的交易指令)。
    * **实现注意事项:** 处理资金和持仓状态更新需要精确和非阻塞。可能包含计算（如头寸调整、风险计算），需异步化。

5.  **Execution Handler (执行处理器):**
    * **职责:** 负责实际执行交易订单，并处理执行结果。
    * **实现:**
        * `SimulatedExecutionHandler`: **在回测中模拟撮合和结算**。接收 `OrderEvent`，记录为待处理。**在接收到下一个时间步 ($T+1$) 的 `MarketEvent` 时**，根据 $T+1$ 的数据模拟撮合前一个时间步 ($T$) 或之前产生的待处理订单，然后发布 `FillEvent`。
        * `BrokerExecutionHandler`: **连接真实经纪商 API**。接收 `OrderEvent`，通过异步 API 发送订单。通过异步回调或单独的数据流接收经纪商的订单状态更新（成交、拒绝等），并转化为 `FillEvent` 或 `OrderUpdateEvent` 发布。其行为完全由经纪商的异步回报驱动。

6.  **Risk Manager (风险管理器 - 可选):**
    * **职责:** 在订单发送到执行器前，对 `OrderEvent` 进行实时风险检查（如资金限制、持仓限制、波动率检查）。
    * **订阅事件:** `OrderEvent` (来自 Portfolio)。
    * **发布事件:** `OrderEvent` (如果通过检查，转发给 Execution Handler) 或记录风险违规。

7.  **Performance Manager (性能管理器 - 可选):**
    * **职责:** 接收 `FillEvent` 和定期的投资组合快照，计算和报告交易策略的绩效指标（收益、回撤、夏普比率等）。
    * **订阅事件:** `FillEvent`，周期性的 Portfolio 状态事件。

**核心事件类型:**

* `MarketEvent`: 新的市场数据（股票、期货等 instrument 的价格、成交量等）。
* `SignalEvent`: 策略发出的交易信号（意图）。
* `OrderEvent`: 由 Portfolio 根据信号和状态生成的具体交易指令。
* `FillEvent`: 订单执行结果（成交价格、数量、费用等）。
* `HeartbeatEvent` (可选): 用于触发周期性任务或检查系统健康。
* `BacktestStartEvent`, `BacktestEndEvent`: 回测流程控制事件。
* `OrderUpdateEvent` (Live): 订单状态变化（如提交成功、部分成交、已取消、已拒绝等）。

**事件流转示意图 (涵盖回测和实盘):**

```
+-----------------+       +---------------+
|   Asyncio       |       | Event Bus     |<--------------------+
|   Event Loop    |------>| (asyncio.Queue|                     |
+-----------------+       | + Dispatcher) |                     |
         ^                +---------------+                     |
         |                       ^   ^                          |
         |                       |   |                          |
+--------+--------+     +----------+---------+     +----------+---------+
| Data Feed       |---->| Strategy         |---->| Portfolio        |
| (Backtest/Live) |     | (Subscribes to   |     | (Subscribes to   |
|                 |     |  MarketEvent)    |     |  SignalEvent,    |
| - Backtest: T   |     +------------------+     |  FillEvent,      |
|   Step & Wait   |              |               |  OrderUpdateEvent)|
|   (await queue  |              v               +----------+---------+
|    empty)       |      +------------------+            |
| - Live: Real-   |      | SignalEvent      |            v
|   time Stream   |      | (Published by    |   +-----+------------+
|   (No Waiting)  |      |  Strategy)       |   | OrderEvent       |
+-----------------+      +------------------+   | (Published by    |
         |                                      |  Portfolio)     |
         |                                      +-----+------------+
         |                                            |
+--------+--------+                                   v
| MarketEvent     |                            +------------------+
| (Published by   |         +------------------+ Risk Manager     |
|  Data Feed) ----|-------->| (Optional)       | (Subscribes to   |
| (Note: T+1 data |         | (Subscribes to   |  OrderEvent)     |
|  triggers settle|         |  OrderEvent)     +------------------+
|  in Simulated   |         +------------------+     | (if passes check)
|  Execution)     |                         |        v
+-----------------+                         |  +----------+---------+
                                            |  | Execution Handler|
                                            |->| (Simulated/Broker)|
                                            |  | (Subscribes to   |
                                            |  |  OrderEvent)     |
                                            |  | - Simulated:     |
                                            |  |   Settles on     |
                                            |  |   NEXT Market    |
                                            |  |   Event (T+1 data)|
                                            |  | - Broker:        |
                                            |  |   Async API comms|
                                            |  |   Receives fills |
                                            |  |   via callbacks/ |
                                            |  |   stream         |
                                            |  +----------+---------+
                                            |           |
                                            |           v
                                            +----+------------------+
                                                 | FillEvent /      |
                                                 | OrderUpdateEvent |
                                                 | (Published by    |
                                                 |  Execution Handler)|
                                                 +------------------+
                                                           ^
                                                           |
                 +----------------------+------------------+
                 | Performance Manager  |
                 | (Optional)           |
                 | (Subscribes to       |
                 |  FillEvent,          |
                 |  Portfolio Updates)  |
                 +----------------------+
```

**回测与实盘的切换:**

框架通过替换 `Data Feed` 和 `Execution Handler` 的具体实现类来无缝切换回测和实盘模式。核心的 `Strategy` 和 `Portfolio` 逻辑可以最大程度地复用，但其内部实现（尤其是涉及耗时计算的部分）必须通过 `asyncio` 异步化手段（如 `run_in_executor`）来保障在高事件率下的性能，这对于实盘尤其关键，同时也是确保回测中“等待队列清空”机制不会死锁的前提。

**性能保障的关键:**

* 所有 I/O 操作必须使用 `await`。
* 所有 CPU 密集型计算必须使用 `asyncio.get_running_loop().run_in_executor()` 或分解为可在协程中协作的小步骤 (`await asyncio.sleep(0)`) 来执行。
* Event Bus 的消费者逻辑必须高效，确保事件能被及时分发。

**优点:**

* **灵活:** 容易添加新的策略、数据源或执行器。
* **模块化:** 组件职责清晰，易于单独测试和维护。
* **高效:** `asyncio` 提供高性能的并发处理能力，特别适合 I/O 密集型任务。
* **回测/实盘统一:** 共享核心逻辑，降低开发和测试成本。
