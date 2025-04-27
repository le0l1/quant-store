## 量化框架设计方案：基于事件驱动 (完整版)

**核心目标:**

*   构建一个灵活、可扩展的事件驱动框架。
*   支持历史数据回测（Backtesting）。
*   支持模拟交易（Paper Trading）。
*   易于集成用户自定义的数据源（Data Source）。
*   易于集成用户自定义的交易策略（Strategy），并支持策略访问历史数据进行决策。

**设计理念:**

*   **事件驱动:** 系统核心是一个事件循环，各模块通过事件进行解耦通信。模块监听并响应特定事件，或将新事件放入事件队列。
*   **模块化:** 功能被划分到独立的模块中，每个模块职责单一。
*   **可扩展性:** 通过定义清晰的接口（例如使用抽象基类 ABC），方便用户替换或添加自定义模块。

---

### 一、 功能模块设计

1.  **事件引擎 (Event Engine)**
    *   **职责:** 框架的核心调度器。
    *   **功能:**
        *   维护一个事件队列（Event Queue），例如 `queue.Queue`。
        *   管理事件类型与对应处理函数（Handler）的注册与注销（通常使用字典）。
        *   运行主事件循环：持续从队列获取事件，并将事件分发给所有已注册监听该事件类型的处理函数。
        *   控制框架的启动、运行和停止。
    *   **关键组件:** 事件队列、事件处理器映射表。

2.  **事件定义 (Events)**
    *   **职责:** 定义系统中流转的各种事件类型及其携带的信息。
    *   **功能:** 每个事件是一个包含特定信息的对象（类的实例）。
    *   **核心事件类型:**
        *   `MarketEvent`: 新的市场数据到达（如 K 线 Bar 数据 - OHLCV, 时间戳, 标的代码；或 Tick 数据）。
        *   `SignalEvent`: 策略模块产生的交易意图信号（标的代码, 信号类型 - 如 LONG/SHORT/EXIT, 强度等）。
        *   `OrderEvent`: 具体的委托订单请求（标的代码, 订单类型 - 如 MKT/LMT, 方向 - BUY/SELL, 数量, 价格（如果是限价单））。
        *   `FillEvent`: 订单成交回报（时间戳, 标的代码, 方向, 成交数量, 成交价格, 佣金, 滑点信息等）。
        *   `PortfolioUpdateEvent`: 投资组合状态更新（时间戳, 总价值, 持仓, 现金等）。
        *   `TimerEvent`: 定时器触发的事件（用于需要按固定时间间隔执行的逻辑）。
        *   `SystemEvent`: 系统状态事件（如 Start, Stop, Error）。

3.  **数据源接口 (DataSource Interface / IDataSource)**
    *   **职责:** 提供原始市场数据流。
    *   **功能:**
        *   定义获取市场数据的标准接口（抽象基类）。
        *   **回测模式 (`BacktestDataSource`):** 从文件（CSV, HDF5 等）、数据库加载历史数据。按历史时间戳顺序生成 `MarketEvent` 并放入事件队列。需要能控制回放速度或仅按事件驱动。
        *   **模拟交易模式 (`LiveDataSource`):** 连接到实时数据接口（如券商 API 的 WebSocket/REST），接收实时数据（Ticks 或 Bars），生成 `MarketEvent` 并放入事件队列。
        *   用户通过实现此接口来接入自定义数据源。
    *   **关键方法:** `stream_next()` 或类似方法驱动数据流（回测），或包含启动/停止连接、订阅标的等方法（实时）。

4.  **数据处理器 (Data Handler)**
    *   **职责:** 缓存市场数据，并为策略提供历史和当前数据访问接口。
    *   **功能:**
        *   监听 `MarketEvent`，将最新的市场数据更新到内部缓存中。
        *   为每个关注的标的维护一个时间序列数据结构（如 Pandas DataFrame 或 NumPy 数组），存储 OHLCV 等信息。
        *   提供接口供策略查询：
            *   `get_latest_bars(symbol, N=1)`: 获取最新的 N 条 Bar 数据。
            *   `get_historical_bars(symbol, end_dt, lookback_period)`: 获取截止到 `end_dt` 的指定回溯期的数据。
            *   `get_current_bar(symbol)`: 获取当前处理中的 Bar 数据。
        *   管理数据缓存的大小和时间窗口。
    *   **交互:** 接收 `MarketEvent`，服务于 `Strategy` 的数据查询请求。

5.  **策略接口 (Strategy Interface / IStrategy)**
    *   **职责:** 实现具体的交易决策逻辑。
    *   **功能:**
        *   定义策略逻辑的标准接口（抽象基类）。
        *   在初始化时接收 `Data Handler` 和 `Event Queue` 的引用。
        *   监听 `MarketEvent` 和/或 `TimerEvent`。
        *   当事件触发时，通过 `Data Handler` 获取所需的当前和历史数据（例如 `data_handler.get_latest_bars(symbol, N=50)` 来获取最近 50 根 K 线）。
        *   基于获取的数据进行计算（指标、模型预测等）和分析。
        *   当满足交易条件时，生成 `SignalEvent` 或直接生成 `OrderEvent`，并将其 `put` 到事件队列。
        *   用户需要继承此接口并实现 `on_event(event)` 或 `on_market_event(event)`, `on_timer_event(event)` 等方法。

6.  **投资组合管理器 (Portfolio Manager)**
    *   **职责:** 跟踪账户状态，管理风险。
    *   **功能:**
        *   管理初始资金。
        *   监听 `SignalEvent` (可选，用于基于信号进行风险检查或仓位调整) 和 `FillEvent`。
        *   处理 `FillEvent`：更新持仓（数量、平均成本）、可用现金、计算已实现盈亏。
        *   监听 `MarketEvent`：更新当前持仓的市值（Mark-to-Market）、计算浮动盈亏、更新投资组合总价值。
        *   提供查询接口：获取当前现金、持仓详情、总资产、历史净值等。
        *   执行基本的订单前检查（如资金是否足够），虽然更复杂的检查可能在`RiskManager`（如果单独设立）或`OrderManager`中。
        *   生成 `PortfolioUpdateEvent` 通知其他模块（如性能分析）。

7.  **订单管理器/执行处理器 (Order Manager / Execution Handler Interface / IExecutionHandler)**
    *   **职责:** 处理 `OrderEvent`，模拟或执行交易。
    *   **功能:**
        *   定义执行处理的标准接口（抽象基类）。
        *   监听 `OrderEvent`。
        *   **回测模式 (`BacktestExecutionHandler`):**
            *   接收 `OrderEvent`。
            *   根据下一可用市场数据（如 `MarketEvent` 带来的下一 Bar 开盘价或特定规则）和预设的滑点（Slippage）、佣金（Commission）模型，模拟订单成交。
            *   生成 `FillEvent`（包含成交详情、成本）并放入事件队列。
            *   **必须避免使用未来数据 (Lookahead Bias)**。
        *   **模拟交易模式 (`SimulatedExecutionHandler`):**
            *   接收 `OrderEvent`。
            *   连接到券商或交易所提供的模拟交易（Paper Trading）API。
            *   将 `OrderEvent` 转换成目标 API 可接受的格式并发送下单请求。
            *   监听来自 API 的订单状态更新和成交回报。
            *   根据收到的成交回报，生成 `FillEvent` 并放入事件队列。
        *   用户可以实现此接口对接不同的模拟交易平台或实盘接口（稍作修改）。

8.  **性能与风险分析 (Performance Analyzer)**
    *   **职责:** 评估策略表现和风险暴露。
    *   **功能:**
        *   监听 `FillEvent` 和 `PortfolioUpdateEvent`。
        *   记录交易历史、每日或定期的投资组合价值（净值曲线）。
        *   在回测结束后或模拟交易过程中定期计算关键绩效指标（KPIs）：
            *   累计收益率、年化收益率
            *   夏普比率、索提诺比率
            *   最大回撤（幅度、持续时间）
            *   胜率、盈亏比
            *   交易频率、平均持仓时间
            *   Alpha, Beta, 信息比率（如果提供基准）
        *   生成可视化图表（净值曲线、回撤图、交易点位等）和统计报告。

9.  **配置模块 (Configuration)**
    *   **职责:** 管理框架和策略的配置参数。
    *   **功能:**
        *   从文件（如 YAML, JSON）或环境变量加载配置。
        *   配置内容：运行模式（backtest/paper）、回测起止时间、初始资金、交易标的列表、数据源配置（类型、路径/API密钥）、策略配置（类名、参数）、执行配置（佣金率、滑点设置、API密钥）、日志级别等。

---

### 二、 工作流 (Workflow)

#### A. 初始化阶段 (Setup)

1.  **加载配置 (Load Config):** 框架启动，读取配置文件（如 `config.yaml`）。
2.  **实例化核心组件 (Instantiate Components):**
    *   创建 `EventEngine`。
    *   根据配置创建具体的 `DataSource` 实例 (`BacktestDataSource` 或 `LiveDataSource`)。
    *   创建 `DataHandler` 实例。
    *   创建 `Portfolio` 实例，设置初始资金。
    *   根据配置创建具体的 `ExecutionHandler` 实例 (`BacktestExecutionHandler` 或 `SimulatedExecutionHandler`)，传入佣金/滑点模型。
    *   根据配置创建用户自定义的 `Strategy` 实例，将 `DataHandler` 和 `EventEngine` (或其 `put_event` 方法/队列引用) 传入。
    *   创建 `Performance` 实例。
3.  **注册事件处理器 (Register Handlers):**
    *   将 `DataHandler` 注册为 `MarketEvent` 的处理器。
    *   将 `Strategy` 注册为 `MarketEvent` 和/或 `TimerEvent` 的处理器。
    *   将 `Portfolio` 注册为 `FillEvent` 和 `MarketEvent` 的处理器。
    *   将 `ExecutionHandler` 注册为 `OrderEvent` 的处理器。
    *   将 `Performance` 注册为 `FillEvent` 和 `PortfolioUpdateEvent` 的处理器。
    *   (根据需要注册其他事件和处理器)
4.  **启动数据流/连接 (Start Data):**
    *   `DataSource` 开始准备数据（如打开文件，或连接到实时 API 并订阅行情）。

#### B. 运行阶段 (Event Loop)

1.  **启动事件引擎 (Start Engine):** 调用 `EventEngine.start()`，进入主循环。
2.  **数据注入 (Data Injection):**
    *   `DataSource` 生成一个新的 `MarketEvent` (基于历史数据时间戳或实时推送)，并将其 `put` 到 `EventEngine` 的事件队列。
3.  **事件处理循环 (Event Processing):**
    *   `EventEngine` 从队列中获取一个事件 (e.g., `MarketEvent`)。
    *   `EventEngine` 将该事件分发给所有已注册监听此事件类型的处理器。
    *   **数据更新:** `DataHandler` 接收 `MarketEvent`，更新其内部数据缓存。
    *   **策略决策:** `Strategy` 接收 `MarketEvent`。在其处理方法中，它调用 `DataHandler` 获取所需历史数据 (`get_latest_bars`)。基于数据进行分析，如果触发条件，生成 `SignalEvent` 或 `OrderEvent` 并 `put` 到事件队列。
    *   **市值更新:** `Portfolio` 接收 `MarketEvent`，更新持仓市值和总资产价值，可能生成 `PortfolioUpdateEvent` 并放入队列。
4.  **订单生命周期:**
    *   `EventEngine` 获取 `OrderEvent` (由 Strategy 直接生成或由 Portfolio/Risk Manager 根据 SignalEvent 生成)。
    *   `EventEngine` 将 `OrderEvent` 分发给 `ExecutionHandler`。
    *   `ExecutionHandler` 处理订单：
        *   **回测:** 模拟成交，生成 `FillEvent`，放入队列。
        *   **模拟:** 发送订单到模拟平台，等待回报。收到成交回报后，生成 `FillEvent`，放入队列。
5.  **成交处理 (Fill Processing):**
    *   `EventEngine` 获取 `FillEvent`。
    *   `EventEngine` 将 `FillEvent` 分发给 `Portfolio` 和 `Performance`。
    *   **状态更新:** `Portfolio` 接收 `FillEvent`，更新现金、持仓、已实现盈亏。
    *   **绩效记录:** `Performance` 接收 `FillEvent`，记录交易详情。
6.  **其他事件:** `EventEngine` 处理队列中的其他事件（如 `TimerEvent`, `PortfolioUpdateEvent`, `SystemEvent`），分发给相应的处理器。
7.  **循环:** `EventEngine` 返回步骤 3，持续处理事件队列中的事件，直到满足停止条件。

#### C. 结束阶段 (Termination)

1.  **停止条件满足 (Stopping Condition):**
    *   **回测:** `DataSource` 发出数据结束信号或不再产生事件，并且事件队列为空。
    *   **模拟交易:** 用户发出停止指令，或发生严重错误。
    *   `EventEngine` 停止事件循环。
2.  **最终计算与报告 (Final Analysis & Reporting):** (主要用于回测)
    *   调用 `Performance` 模块的方法，基于整个过程记录的数据计算最终的性能指标。
    *   生成回测报告（图表、统计数据）。
3.  **资源清理 (Cleanup):**
    *   关闭 `DataSource` 的连接或文件句柄。
    *   关闭 `ExecutionHandler` 的 API 连接（如果适用）。
    *   保存需要持久化的数据（如交易记录、净值曲线）。

---

### 三、 关键考虑点

*   **时间处理:** 回测中时间必须严格单调递增，由 `DataSource` 控制。实时模式使用数据源或系统时间。
*   **避免前视偏差 (Lookahead Bias):** 回测时，策略决策只能使用当前事件及之前的数据。成交模拟通常使用信号产生后的下一个可用价格。`DataHandler` 的数据获取接口设计也需注意这点。
*   **滑点与佣金:** `ExecutionHandler` 的回测实现需要灵活配置和模拟这些成本。
*   **数据频率:** 框架应能处理不同频率的数据（Tick, 秒级, 分钟级, 日线等），事件和数据结构需相应支持。
*   **并发与性能:** 实时数据处理和 API 交互可能受益于异步编程 (`asyncio`)。回测计算密集部分可能受 GIL 限制，考虑多进程或优化计算库 (NumPy, Pandas)。
*   **错误处理与日志:** 完善的日志记录对于调试至关重要。健壮的错误处理机制能防止框架意外崩溃。
*   **状态一致性:** 确保 `Portfolio` 等状态管理模块在事件处理中的更新是正确的。

---

这个设计方案提供了一个较为完整且可扩展的基础。用户只需编写符合 `IDataSource` 和 `IStrategy` 接口的类，并进行相应配置，即可利用此框架进行量化策略的回测与模拟交易。