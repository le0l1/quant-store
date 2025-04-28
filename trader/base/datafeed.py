from abc import ABC, abstractmethod
from typing import Optional

from trader.base.event import Event
from trader.base.event_engine import EventEngine

class DataFeed(ABC):
    """Abstract base class for data feeds in quantitative trading framework"""
    
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.is_active = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source"""
        pass

    def push_market_event(self, event_type: str, data: dict) -> None:
        """Push a market event to the event engine"""
        event = MarketEvent(event_type=event_type, data=data)
        self.event_engine.put(event)

    async def start(self) -> None:
        """Start the data feed"""
        if not self.is_active:
            await self.connect()
            self.is_active = True

    async def stop(self) -> None:
        """Stop the data feed"""
        if self.is_active:
            await self.disconnect()
            self.is_active = False