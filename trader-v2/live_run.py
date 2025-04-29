# live_run.py
import asyncio
import logging
from datetime import datetime, timezone
import signal # Import signal module for graceful shutdown

# 导入所有组件和事件
from core.events import (
    MarketEvent, SignalEvent, OrderEvent, FillEvent, HeartbeatEvent,
    Event
)
from core.event_bus import EventBus
# 导入实盘组件
from components.data_feed import LiveDataFeed # Use LiveDataFeed
from components.strategy import ExampleStrategy # Strategy remains the same
from components.portfolio import ExamplePortfolio # Portfolio remains the same
from components.broker_execution_handler import BrokerExecutionHandler # Use BrokerExecutionHandler


# 配置日志
# INFO level is good for seeing main events; DEBUG if you need more detail.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- API 配置 (Placeholder) ---
# In a real application, load this from a config file (YAML, JSON, environment variables)
LIVE_DATA_API_CONFIG = {
    "websocket_url": "wss://mock-data-api.example.com/ws",
    "api_key": "YOUR_DATA_API_KEY",
    # ... other config
}

BROKER_API_CONFIG = {
    "api_url": "https://mock-broker-api.example.com/api",
    "websocket_url": "wss://mock-broker-api.example.com/ws",
    "api_key": "YOUR_BROKER_API_KEY",
    "api_secret": "YOUR_BROKER_API_SECRET",
    "account_id": "YOUR_ACCOUNT_ID",
    # ... other config
}

# --- 主运行函数 ---

async def main():
    """Main asynchronous function to set up and run the framework for live trading."""
    logger.info("Starting the trading framework in LIVE mode...")

    # 1. 创建 Event Bus 实例
    bus = EventBus()

    # 2. 创建组件实例并注入 Event Bus 和 API 配置
    # 注意这里使用的是 LiveDataFeed 和 BrokerExecutionHandler
    data_feed = LiveDataFeed(bus, api_config=LIVE_DATA_API_CONFIG)
    strategy = ExampleStrategy(bus) # Reuse the same strategy logic
    portfolio = ExamplePortfolio(bus) # Reuse the same portfolio logic
    execution_handler = BrokerExecutionHandler(bus, api_config=BROKER_API_CONFIG) # Use the broker handler

    # 3. 启动 Event Bus 运行任务
    bus_task = asyncio.create_task(bus.run())
    logger.info("Event Bus run task created.")

    # 4. 连接到经纪商 API
    logger.info("Connecting to broker...")
    await execution_handler.connect()
    if not execution_handler._connected:
         logger.error("Failed to connect to broker. Aborting.")
         bus_task.cancel()
         await asyncio.gather(bus_task, return_exceptions=True)
         return # Exit if connection failed

    # 5. 启动 Data Feed 任务并订阅数据
    # Live data feed runs continuously and pushes data as it arrives
    logger.info("Starting Data Feed...")
    data_feed_task = asyncio.create_task(data_feed.start_feed())

    # Subscribe to necessary symbols after data feed is potentially connected/ready
    # In a real implementation, you might wait for a 'connected' event from data_feed
    # For this mock, we call subscribe after starting the task.
    await asyncio.sleep(0.1) # Give start_feed a moment to potentially connect
    await data_feed.subscribe_symbols(['AAPL', 'MSFT']) # Example symbols

    # 6. 保持主程序运行，直到外部中断 (如 Ctrl+C)
    # 在实盘模式下，框架持续运行，直到被显式停止。
    # 我们可以让 main 协程等待一个 Future，直到它被取消。
    logger.info("Framework is running in LIVE mode. Press Ctrl+C to stop.")
    try:
        # asyncio.Future() creates a Future that is not done. Awaiting it
        # will suspend the coroutine indefinitely until the Future is cancelled.
        # This keeps the main coroutine alive while other tasks (bus_task, data_feed_task) run.
        await asyncio.Future()
    except asyncio.CancelledError:
        # This exception is raised when asyncio.run() receives a signal like Ctrl+C
        logger.info("Ctrl+C received. Shutting down...")

    # --- Graceful Shutdown ---
    logger.info("Initiating graceful shutdown...")

    # Stop the data feed first (it will stop publishing events)
    await data_feed.stop_feed()
    # Cancel the data feed task if it's still running (stop_feed might not exit the task immediately)
    data_feed_task.cancel()
    try:
        await data_feed_task
    except asyncio.CancelledError:
        pass # Task was cancelled as expected
    logger.info("Data Feed stopped.")

    # Disconnect from the broker
    await execution_handler.disconnect()
    logger.info("Broker disconnected.")

    # Stop the Event Bus (it will process any remaining events in the queue)
    # Note: In live trading, you might NOT want to wait for the queue to be empty
    # if it's backed up, you might just stop. But for a clean shutdown example:
    # await bus.stop() # The bus.stop() implementation waits for queue.join()
    # Let's just cancel the bus task directly after stopping data feed/broker
    # as we don't have critical end-of-data events like in backtesting.
    bus_task.cancel()
    try:
        await bus_task
    except asyncio.CancelledError:
        logger.info("Event Bus run task was cancelled during shutdown.")


    logger.info("Trading framework LIVE mode finished.")


if __name__ == "__main__":
    asyncio.run(main())