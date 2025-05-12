# components/data_feed.py
import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import MarketEvent, BacktestStartEvent, BacktestEndEvent

logger = logging.getLogger(__name__)

class BaseDataFeed(BaseComponent):
    """
    Base class for Data Feed components.
    Responsible for providing market data.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        self._running = False # State to control the data feed loop

    async def start_feed(self):
        """
        Starts the data feed process.
        This method should be overridden by subclasses.
        """
        logger.info("BaseDataFeed started. Override start_feed in subclass.")
        self._running = True
        # Subclasses will implement the loop to fetch/publish data

    async def stop_feed(self):
        """
        Stops the data feed process gracefully.
        This method should be overridden by subclasses.
        """
        logger.info("BaseDataFeed stopping. Override stop_feed in subclass.")
        self._running = False
        # Subclasses will implement logic to stop their data source (e.g., close websocket)
    def get_historical_prices(self, period) -> pd.DataFrame:
        pass

    def get_latest_price(self, symbol: str) -> Dict[str, Any]:
        """
        Get the latest price for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g. 'BTC/USD')
            
        Returns:
            Dictionary containing price and timestamp:
            {
                'price': float,
                'timestamp': datetime
            }
        """
        raise NotImplementedError("Subclasses must implement get_latest_price")
