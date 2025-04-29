# components/strategy.py
import logging
import asyncio # Import asyncio for potential async operations
from typing import Any, Dict

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import MarketEvent, SignalEvent # Import events strategy interacts with
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseStrategy(BaseComponent):
    """
    Base class for trading strategies.
    Strategies subscribe to MarketEvents and publish SignalEvents.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")

    def _setup_event_handlers(self):
        """Register strategy's event handlers."""
        # Strategies typically listen to MarketEvents
        self.event_bus.subscribe(MarketEvent, self._on_market_event)
        # Could also listen to FillEvents, OrderEvents if strategy needs to react to execution
        # self.event_bus.subscribe(FillEvent, self._on_fill_event)

    async def _on_market_event(self, event: MarketEvent):
        """Internal handler for MarketEvents. Calls the user-defined logic."""
        # Ensure non-blocking processing here. If on_market_data is CPU-bound,
        # it MUST use run_in_executor or async/await within its logic.
        await self.on_market_data(event.symbol, event.timestamp, event.data)

    # async def _on_fill_event(self, event: FillEvent):
    #     """Internal handler for FillEvents."""
    #     await self.on_fill(event) # Call user-defined fill handling logic

    async def on_market_data(self, symbol: str, timestamp: datetime, data: Dict[str, Any]):
        """
        This method contains the strategy's core logic.
        Override this in subclasses.
        """
        # Example: print(f"Strategy received market data for {symbol} at {timestamp}")
        pass # Subclasses must override


# --- Example Strategy Implementation ---

class ExampleStrategy(BaseStrategy):
    """
    An example strategy that always generates a LONG signal for AAPL
    when it receives market data for AAPL.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        self._traded_aapl = False # Simple flag to trade only once for demo

    async def on_market_data(self, symbol: str, timestamp: datetime, data: Dict[str, Any]):
        """Implement example strategy logic."""
        # logger.debug(f"ExampleStrategy considering data for {symbol} at {timestamp}")

        # Simple logic: If it's AAPL data and we haven't traded yet, publish a LONG signal
        if symbol == 'AAPL' and not self._traded_aapl:
            logger.info(f"ExampleStrategy: Found AAPL data at {timestamp}. Generating LONG signal.")
            signal_event = SignalEvent(symbol=symbol, direction="LONG")
            self.event_bus.publish(signal_event)
            self._traded_aapl = True # Only signal once