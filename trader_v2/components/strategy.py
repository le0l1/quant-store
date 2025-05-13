# components/strategy.py
import logging

from components.base import BaseComponent
from components.portfolio import BaseBroker
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
        portfolio: BaseBroker,
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

    def get_histroy(self, period: int) -> pd.DataFrame:
        return self.data_feed.get_historical_prices(
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
        self.index = 1
        logger.info(f"{self.__class__.__name__} initialized with period={self.momentum_period}, weight={self.default_weight}.")

    async def on_market_data(self, event: MarketEvent):
        self.index += 1
        if self.index % 10 != 1:
            return

        history_df = self.get_histroy(self.momentum_period + 1) # Get historical prices without symbol
        if history_df is None or len(history_df) < self.momentum_period + 1:
            return

        symbols = history_df['symbol'].unique()
        
        # Calculate momentum for each symbol
        momentum_dict = {}
        for symbol in symbols:
            symbol_df = history_df[history_df['symbol'] == symbol]
            momentum = symbol_df.iloc[0]['close'] - symbol_df.iloc[-1]['close']
            momentum_dict[symbol] = momentum
        
        # Sort symbols by momentum (descending order)
        sorted_momentum = sorted(momentum_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Get top 3 momentum symbols
        top_3_symbols = [x[0] for x in sorted_momentum[:3]]

        # Handle positions for all symbols
        for symbol in symbols:
            has_position = self.portfolio.get_current_position_quantity(symbol) != 0

            if not symbol in top_3_symbols:
                if has_position:
                    direction = "FLAT"
                    signal_event = SignalEvent(symbol=symbol, direction=direction)
                    self.event_bus.publish(signal_event)
            else:
                if not has_position:
                    direction = "LONG"
                    weight = self.default_weight
                    signal_event = SignalEvent(symbol=symbol, direction=direction, weight=weight)
                    self.event_bus.publish(signal_event)
