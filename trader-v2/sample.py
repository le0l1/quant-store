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
from components.data_feed import MockDataFeed
from components.strategy import MomentumStrategy
from components.portfolio import MomentumPortfolio
from components.execution_handler import SimulatedExecutionHandler # Import SimulatedExecutionHandler


# 配置日志
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("MomentumStrategy").setLevel(logging.DEBUG)
logging.getLogger("components.portfolio").setLevel(logging.DEBUG)
logging.getLogger("components.execution_handler").setLevel(logging.DEBUG) # Keep DEBUG for execution details


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# --- 主运行函数 ---

async def main():
    """Main asynchronous function to set up and run the framework for backtesting."""
    logger.debug("Starting the trading framework in BACKTEST mode...")

    # 1. 创建 Event Bus 实例
    bus = EventBus()

    # 2. 模拟一些历史市场数据用于 MockDataFeed
    mock_data = [
        # AAPL Data (at least 21 points needed)
        {'timestamp': datetime(2023, 1, 1, 9, 0, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 100.0, 'close': 100.0},
        {'timestamp': datetime(2023, 1, 1, 9, 1, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 100.0, 'close': 101.0},
        {'timestamp': datetime(2023, 1, 1, 9, 2, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 101.0, 'close': 100.5},
        {'timestamp': datetime(2023, 1, 1, 9, 3, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 100.5, 'close': 101.5},
        {'timestamp': datetime(2023, 1, 1, 9, 4, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 101.5, 'close': 102.0},
        {'timestamp': datetime(2023, 1, 1, 9, 5, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 102.0, 'close': 102.5},
        {'timestamp': datetime(2023, 1, 1, 9, 6, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 102.5, 'close': 102.8},
        {'timestamp': datetime(2023, 1, 1, 9, 7, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 102.8, 'close': 103.0},
        {'timestamp': datetime(2023, 1, 1, 9, 8, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 103.0, 'close': 102.9},
        {'timestamp': datetime(2023, 1, 1, 9, 9, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 102.9, 'close': 103.5},
        {'timestamp': datetime(2023, 1, 1, 9, 10, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 103.5, 'close': 104.0},
        {'timestamp': datetime(2023, 1, 1, 9, 11, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 104.0, 'close': 104.2},
        {'timestamp': datetime(2023, 1, 1, 9, 12, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 104.2, 'close': 104.5},
        {'timestamp': datetime(2023, 1, 1, 9, 13, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 104.5, 'close': 104.8},
        {'timestamp': datetime(2023, 1, 1, 9, 14, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 104.8, 'close': 105.0},
        {'timestamp': datetime(2023, 1, 1, 9, 15, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 105.0, 'close': 105.2},
        {'timestamp': datetime(2023, 1, 1, 9, 16, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 105.2, 'close': 105.5},
        {'timestamp': datetime(2023, 1, 1, 9, 17, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 105.5, 'close': 105.8},
        {'timestamp': datetime(2023, 1, 1, 9, 18, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 105.8, 'close': 106.0},
        {'timestamp': datetime(2023, 1, 1, 9, 19, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 106.0, 'close': 106.2},
        {'timestamp': datetime(2023, 1, 1, 9, 20, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 106.2, 'close': 106.5}, # 21st point
        {'timestamp': datetime(2023, 1, 1, 9, 21, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 106.5, 'close': 106.4}, # Still positive momentum
        {'timestamp': datetime(2023, 1, 1, 9, 22, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 106.4, 'close': 105.0}, # Momentum might become negative
        {'timestamp': datetime(2023, 1, 1, 9, 23, 0, tzinfo=timezone.utc), 'symbol': 'AAPL', 'open': 105.0, 'close': 104.0}, # Likely negative momentum

        # MSFT Data (at least 21 points needed)
        {'timestamp': datetime(2023, 1, 1, 9, 0, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 200.0, 'close': 200.0},
        {'timestamp': datetime(2023, 1, 1, 9, 1, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 200.0, 'close': 201.0},
        {'timestamp': datetime(2023, 1, 1, 9, 2, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 201.0, 'close': 200.8},
        {'timestamp': datetime(2023, 1, 1, 9, 3, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 200.8, 'close': 201.2},
        {'timestamp': datetime(2023, 1, 1, 9, 4, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 201.2, 'close': 201.5},
        {'timestamp': datetime(2023, 1, 1, 9, 5, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 201.5, 'close': 201.8},
        {'timestamp': datetime(2023, 1, 1, 9, 6, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 201.8, 'close': 202.0},
        {'timestamp': datetime(2023, 1, 1, 9, 7, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 202.0, 'close': 201.9},
        {'timestamp': datetime(2023, 1, 1, 9, 8, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 201.9, 'close': 202.5},
        {'timestamp': datetime(2023, 1, 1, 9, 9, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 202.5, 'close': 203.0},
        {'timestamp': datetime(2023, 1, 1, 9, 10, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 203.0, 'close': 203.2},
        {'timestamp': datetime(2023, 1, 1, 9, 11, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 203.2, 'close': 203.5},
        {'timestamp': datetime(2023, 1, 1, 9, 12, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 203.5, 'close': 203.8},
        {'timestamp': datetime(2023, 1, 1, 9, 13, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 203.8, 'close': 204.0},
        {'timestamp': datetime(2023, 1, 1, 9, 14, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 204.0, 'close': 204.2},
        {'timestamp': datetime(2023, 1, 1, 9, 15, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 204.2, 'close': 204.5},
        {'timestamp': datetime(2023, 1, 1, 9, 16, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 204.5, 'close': 204.8},
        {'timestamp': datetime(2023, 1, 1, 9, 17, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 204.8, 'close': 205.0},
        {'timestamp': datetime(2023, 1, 1, 9, 18, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 205.0, 'close': 205.2},
        {'timestamp': datetime(2023, 1, 1, 9, 19, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 205.2, 'close': 205.5},
        {'timestamp': datetime(2023, 1, 1, 9, 20, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 205.5, 'close': 205.8}, # 21st point
        {'timestamp': datetime(2023, 1, 1, 9, 21, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 205.8, 'close': 206.0},
        {'timestamp': datetime(2023, 1, 1, 9, 22, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 206.0, 'close': 206.5},
        {'timestamp': datetime(2023, 1, 1, 9, 23, 5, tzinfo=timezone.utc), 'symbol': 'MSFT', 'open': 206.5, 'close': 207.0},
    ]
    mock_data = sorted(mock_data, key=lambda x: x['timestamp'])


    # 3. 创建组件实例并注入 Event Bus
    data_feed = MockDataFeed(bus, market_data=mock_data)
    strategy = MomentumStrategy(bus, momentum_period=5, default_weight=0.5)
    portfolio = MomentumPortfolio(bus, initial_cash=100000.0, lot_size=10)
    # --- Add commission and slippage parameters to SimulatedExecutionHandler ---
    # Example: 0.1% commission, 0.05% slippage
    execution_handler = SimulatedExecutionHandler(bus, commission_percent=0.001, slippage_percent=0.0005)
    # -------------------------------------------------------------------------


    # 4. 启动 Event Bus 运行任务
    bus_task = asyncio.create_task(bus.run())
    logger.info("Event Bus run task created.")

    # 5. 启动回测流程
    start_time = mock_data[0]['timestamp'] if mock_data else datetime.utcnow().replace(tzinfo=timezone.utc)
    end_time = mock_data[-1]['timestamp'] if mock_data else datetime.utcnow().replace(tzinfo=timezone.utc)
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