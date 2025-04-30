import pandas as pd
from components.data_feed import BaseDataFeed
from core.events import MarketEvent
from core.event_bus import EventBus

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
            await self.event_bus.wait_until_queue_empty()
            
            # Move to the next timestamp
            self._current_index += len(same_timestamp_data)
            
    def stop_feed(self):
        self._is_running = False
        self._df = None
    
    
        
