# components/strategy.py
import logging
import asyncio # Import asyncio for potential async operations
from typing import Any, Dict

from components.base import BaseComponent
from components.portfolio import BasePortfolio
from components.data_feed import BaseDataFeed
from core.event_bus import EventBus
from core.events import MarketEvent, SignalEvent # Import events strategy interacts with
from datetime import datetime
import pandas as pd


logger = logging.getLogger(__name__)

class BaseStrategy(BaseComponent):
    """
    Base class for trading strategies.
    Strategies subscribe to MarketEvents and publish SignalEvents.
    """
    def __init__(self, 
        event_bus: EventBus, 
        data_feed: BaseDataFeed,
        portfolio: BasePortfolio,
        **kwargs
    ):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")
        self.portfolio = portfolio
        self.data_feed = data_feed
        self.params = {}
        self.params.update(kwargs)
        self._last_market_timestamp: datetime = None
        
        self.on_init()

    def on_init(self):
        pass

    def _setup_event_handlers(self):
        self.event_bus.subscribe(MarketEvent, self._on_market_event)

    async def _on_market_event(self, event: MarketEvent):
        await self.on_market_data(event) # Call user-defined market data handling logic
        self._last_market_timestamp = event.timestamp

    async def on_market_data(self, market_event: MarketEvent):
        pass

    def get_histroy(self, symbol: str, period: int) -> pd.DataFrame:
        return self.data_feed.get_historical_prices(
            symbol,
            period,
        )


class MomentumStrategy(BaseStrategy):
    """
    A strategy that calculates 20-period momentum and generates signals.
    Suggests a fixed weight (e.g., 10%) for LONG/SHORT signals.
    """
    def on_init(self):
        self.momentum_period = self.params.get('momentum_period', 20)
        self.default_weight = self.params.get('default_weight', 0.10)
        logger.info(f"{self.__class__.__name__} initialized with period={self.momentum_period}, weight={self.default_weight}.")

    async def on_market_data(self, market_event: MarketEvent):
        symbol = market_event.symbol
        timestamp = market_event.timestamp
        data = market_event.data

        # Ensure we have closing price data
        if 'close' not in data:
            return # Cannot calculate momentum without closing price

        current_price = data['close']


        history_df = self.get_histroy(symbol, self.momentum_period + 1) # Get historical prices for the symbol
        if history_df is None or len(history_df) < self.momentum_period + 1:
            return

        # Calculate momentum (Current Price - Price N periods ago)
        momentum = history_df.iloc[0].close - history_df.iloc[-1].close
        logger.debug(f"Strategy Received Market Event at {timestamp}")
        logger.debug(history_df)

        # Determine signal direction and weight based on momentum
        direction: str
        weight: Optional[float] = None # Default to no weight (or FLAT signal)

        has_position = self.portfolio.get_projected_position_quantity(symbol) != 0

        if momentum > 0 and not has_position:
            direction = "LONG"
            weight = self.default_weight # Suggest default_weight for LONG
            logger.info(f"MomentumStrategy: 做多 {symbol} at {timestamp} ")
            signal_event = SignalEvent(symbol=symbol, direction=direction, weight=weight)
            self.event_bus.publish(signal_event)

        elif momentum < 0 and has_position:
            direction = "FLAT"
            weight = self.default_weight # Suggest default_weight for SHORT
            logger.info(f"MomentumStrategy: 平多 {symbol} at {timestamp} ")
            signal_event = SignalEvent(symbol=symbol, direction=direction)
            self.event_bus.publish(signal_event)
