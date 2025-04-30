# main.py
import asyncio
import logging
from datetime import datetime, timezone
import uuid

# 导入所有组件和事件
from core.events import (
    MarketEvent, SignalEvent, OrderEvent, FillEvent, HeartbeatEvent,
    BacktestStartEvent, BacktestEndEvent, Event
)
from core.event_bus import EventBus
from components.csv_data_feed import CSVDataFeed
from components.strategy import MomentumStrategy
from components.portfolio import MomentumPortfolio
from components.execution_handler import SimulatedExecutionHandler # Import SimulatedExecutionHandler


# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='app.log',  # 输出到文件
    filemode='w',
    force=True  # 如果有其他地方初始化了 logging，这里强制覆盖
)

logging.info('测试日志输出到文件')


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# --- 主运行函数 ---

async def main():
    """Main asynchronous function to set up and run the framework for backtesting."""
    logger.debug("Starting the trading framework in BACKTEST mode...")

    # 1. 创建 Event Bus 实例
    bus = EventBus()



    # 3. 创建组件实例并注入 Event Bus
    data_feed = CSVDataFeed(bus, csv_file='etf.csv')
    portfolio = MomentumPortfolio(bus, initial_cash=100000.0, lot_size=100)
    strategy = MomentumStrategy(bus, portfolio, momentum_period=20, default_weight=0.5)
    # --- Add commission and slippage parameters to SimulatedExecutionHandler ---
    # Example: 0.1% commission, 0.05% slippage
    execution_handler = SimulatedExecutionHandler(bus, commission_percent=0.001, slippage_percent=0.0005)
    # -------------------------------------------------------------------------


    # 4. 启动 Event Bus 运行任务
    bus_task = asyncio.create_task(bus.run())
    logger.info("Event Bus run task created.")

    # 5. 启动回测流程
    start_time = '2023-01-01'
    end_time = '2023-01-31'
    bus.publish(BacktestStartEvent(start_time=start_time, end_time=end_time))
    await bus.wait_until_queue_empty()

    data_feed_task = asyncio.create_task(data_feed.start_feed())

    # 6. 等待 Data Feed 任务完成
    await data_feed_task
    logger.info("Data feed task finished.")

    # 7. 等待 Event Bus 队列最终清空
    logger.info("Waiting for all events in the queue to be processed after data feed finished...")
    await bus.wait_until_queue_empty()
    logger.info("Event queue is finally empty.")

    # 8. 停止 Event Bus
    logger.info("Stopping Event Bus...")
    bus_task.cancel()
    try:
        await bus_task
    except asyncio.CancelledError:
        logger.info("Event Bus run task was cancelled successfully.")

    logger.info("Trading framework main process finished.")


if __name__ == "__main__":
    asyncio.run(main())