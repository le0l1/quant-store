# main.py
import asyncio
import logging
from datetime import datetime, timezone
import uuid

# 导入所有组件和事件
from core.events import (
    BacktestStartEvent, BacktestEndEvent, Event, FillEvent
)
from core.event_bus import EventBus
from components.data_feed import MockDataFeed
from components.strategy import ExampleStrategy
from components.portfolio import ExamplePortfolio
# 导入修改后的执行处理器
from components.execution_handler import SimulatedExecutionHandler


# 配置日志
# 提高日志级别到 INFO，以便更容易看到主要的事件流转和组件行为
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 简单的事件处理函数 (仅保留用于未被组件订阅的事件，或者作为全局监听) ---
# 在组件中订阅事件是推荐的方式。这些可以删除或移到更通用的地方。
# 为了演示，我们可以保留一些全局处理器，但知道组件内部也在处理。
# 暂时保留，但请注意它们可能与组件的日志重复或以不同顺序出现。

async def handle_any_event(event: Event):
    """Global handler to log any event not specifically handled."""
    # logger.debug(f"Global Handler: Received event type: {event.type}")
    pass # Keep it quiet for now, specific handlers are more informative


async def handle_fill_event_global(event: FillEvent):
     """Global handler for FillEvent (e.g., for performance tracking later)."""
     # logger.info(f"Global Fill Handler: {event.symbol} {event.direction} {event.quantity} at {event.price}")
     pass # Portfolio handles fills, this is just an example of another listener


# --- 主运行函数 ---

async def main():
    """Main asynchronous function to set up and run the framework."""
    logger.info("Starting the trading framework main process...")

    # 1. 创建 Event Bus 实例
    bus = EventBus()

    # 2. 模拟一些历史市场数据用于 MockDataFeed
    # Ensure data is sorted by timestamp.
    mock_data = [
        {'timestamp': datetime(2023, 1, 1, 9, 30, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 150.0, 'high': 151.0, 'low': 149.5, 'close': 150.5, 'volume': 100000},
        {'timestamp': datetime(2023, 1, 1, 9, 30, 0, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 250.0, 'high': 251.0, 'low': 249.5, 'close': 250.8, 'volume': 80000},
        {'timestamp': datetime(2023, 1, 1, 9, 31, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 150.5, 'high': 151.5, 'low': 150.0, 'close': 151.2, 'volume': 120000},
        {'timestamp': datetime(2023, 1, 1, 9, 31, 0, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 250.8, 'high': 251.8, 'low': 250.2, 'close': 251.5, 'volume': 70000},
        {'timestamp': datetime(2023, 1, 1, 9, 32, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 151.2, 'high': 152.0, 'low': 151.0, 'close': 151.8, 'volume': 90000},
        {'timestamp': datetime(2023, 1, 1, 9, 32, 0, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 251.5, 'high': 252.5, 'low': 251.0, 'close': 252.0, 'volume': 95000},
         {'timestamp': datetime(2023, 1, 1, 9, 33, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 151.8, 'high': 152.5, 'low': 151.5, 'close': 152.3, 'volume': 110000},
         {'timestamp': datetime(2023, 1, 1, 9, 33, 0, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 252.0, 'high': 253.0, 'low': 251.8, 'close': 252.5, 'volume': 105000},
         {'timestamp': datetime(2023, 1, 1, 9, 34, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 152.3, 'high': 152.8, 'low': 152.0, 'close': 152.5, 'volume': 85000},
         {'timestamp': datetime(2023, 1, 1, 9, 34, 0, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 252.5, 'high': 253.5, 'low': 252.3, 'close': 253.0, 'volume': 115000},
    ]
    mock_data = sorted(mock_data, key=lambda x: x['timestamp'])


    # 3. 创建组件实例并注入 Event Bus
    data_feed = MockDataFeed(bus, market_data=mock_data)
    strategy = ExampleStrategy(bus)
    portfolio = ExamplePortfolio(bus)
    # 注意：这里实例化的是 SimulatedExecutionHandler
    execution_handler = SimulatedExecutionHandler(bus) # Use the simulated handler

    # 4. 启动 Event Bus 运行任务
    bus_task = asyncio.create_task(bus.run())
    logger.info("Event Bus run task created.")

    # 5. 启动回测流程 (发布 BacktestStartEvent 并启动 Data Feed)
    logger.info("Starting Backtest simulation...")
    start_time = mock_data[0]['timestamp'] if mock_data else datetime.utcnow().replace(tzinfo=timezone.utc)
    end_time = mock_data[-1]['timestamp'] if mock_data else datetime.utcnow().replace(tzinfo=timezone.utc)
    bus.publish(BacktestStartEvent(start_time=start_time, end_time=end_time))
    await bus.wait_until_queue_empty() # Wait for BacktestStartEvent to be handled

    # Start the Data Feed (which will drive the simulation by publishing MarketEvents)
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