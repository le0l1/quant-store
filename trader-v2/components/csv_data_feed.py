import pandas as pd
from components.data_feed import BaseDataFeed
from core.events import MarketEvent
from core.event_bus import EventBus
import asyncio

import logging

# 配置日志
logger = logging.getLogger(__name__)


class CSVDataFeed(BaseDataFeed):
    def __init__(self, event_bus: EventBus, csv_file: str):
        super().__init__(event_bus)
        self.csv_file = csv_file
        self._df = pd.read_csv(csv_file)
        self._df = self._df.sort_values(by='timestamp')
        self._df = self._df.set_index('timestamp')

    async def start_feed(self):
        logger.info(f"CSVDataFeed started. Reading from {self.csv_file}")
        self._is_running = True
        self._current_index = 0

        while self._is_running and self._current_index < len(self._df):
            # Get current timestamp
            current_timestamp = self._df.index[self._current_index]
            
            # Find all data points with the same timestamp
            same_timestamp_mask = self._df.index == current_timestamp
            same_timestamp_data = self._df[same_timestamp_mask]
            
            # Put all data points with the same timestamp into the queue
            for _, data_point in same_timestamp_data.iterrows():
                event_data = {
                    'timestamp': current_timestamp,
                    'symbol': data_point['symbol'],
                    'data': data_point
                }
                event = MarketEvent(**event_data)
                self.event_bus.publish(event)
            
            # Wait for all events to be processed
            await asyncio.sleep(0)
            
            # Move to the next timestamp
            self._current_index += len(same_timestamp_data)
            
    def stop_feed(self):
        self._is_running = False
        self._df = None
    
    def get_historical_prices(self, symbol: str, period: int) -> pd.DataFrame:
        if not self._is_running or self._current_index >= len(self._df):
            logger.warning("Data feed is not running or exhausted.")
            return None

        current_timestamp = self._df.index[self._current_index]

        if symbol not in self._df['symbol'].values:
            logger.warning(f"Symbol {symbol} not found in the data feed.")
            return None
        if period <= 0:
            logger.warning(f"Invalid period {period}. Must be greater than 0.")
            return None

        # 过滤截至当前时间的数据
        mask = (self._df['symbol'] == symbol) & (self._df.index <= current_timestamp)
        historical_data = self._df[mask].tail(period)

        if len(historical_data) < period:
            logger.warning(f"Not enough data for symbol {symbol} to get {period} periods.")

        return historical_data[::-1]
    
    
        
