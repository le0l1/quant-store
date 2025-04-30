# components/strategy.py
import logging
import asyncio # Import asyncio for potential async operations
from typing import Any, Dict

from components.base import BaseComponent
from components.portfolio import BasePortfolio
from core.event_bus import EventBus
from core.events import MarketEvent, SignalEvent # Import events strategy interacts with
from datetime import datetime
from collections import deque


logger = logging.getLogger(__name__)

class BaseStrategy(BaseComponent):
    """
    Base class for trading strategies.
    Strategies subscribe to MarketEvents and publish SignalEvents.
    """
    def __init__(self, 
        event_bus: EventBus, 
        portfolio: BasePortfolio,
        **kwargs
    ):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")
        self.portfolio = portfolio
        self.params = {}
        self.params.update(kwargs)
        self.on_init()

    def on_init(self):
        pass

    def _setup_event_handlers(self):
        """Register strategy's event handlers."""
        # Strategies typically listen to MarketEvents
        self.event_bus.subscribe(MarketEvent, self._on_market_event)
        # Could also listen to FillEvents, OrderEvents if strategy needs to react to execution
        # self.event_bus.subscribe(FillEvent, self._on_fill_event)

    async def _on_market_event(self, event: MarketEvent):
        await self.on_market_data(event) # Call user-defined market data handling logic

    async def on_market_data(self, market_event: MarketEvent):
        pass

    # async def _on_fill_event(self, event: FillEvent):
    #     """Internal handler for FillEvents."""
    #     await self.on_fill(event) # Call user-defined fill handling logic


class MomentumStrategy(BaseStrategy):
    """
    A strategy that calculates 20-period momentum and generates signals.
    Suggests a fixed weight (e.g., 10%) for LONG/SHORT signals.
    """
    def on_init(self):
        self.momentum_period = self.params.get('momentum_period', 20)
        self.default_weight = self.params.get('default_weight', 0.10)
        self._historical_prices: Dict[str, deque[float]] = {}
        logger.info(f"{self.__class__.__name__} initialized with period={self.momentum_period}, weight={self.default_weight}.")

    async def on_market_data(self, market_event: MarketEvent):
        """
        Process incoming MarketEvents, update history, calculate momentum, and generate signals.
        """
        symbol = market_event.symbol
        timestamp = market_event.timestamp
        data = market_event.data

        # Ensure we have closing price data
        if 'close' not in data:
            return # Cannot calculate momentum without closing price

        current_price = data['close']

        # Get or create the price history deque for this symbol
        if symbol not in self._historical_prices:
            # We need N+1 data points to calculate the difference between T and T-N
            # So, for 20-period momentum, we need 21 data points.
            self._historical_prices[symbol] = deque(maxlen=self.momentum_period + 1)

        # Append the current closing price
        self._historical_prices[symbol].append(current_price)


        # Check if we have enough history to calculate momentum
        if len(self._historical_prices[symbol]) > self.momentum_period:
            # Get the price from N periods ago (the oldest price in the deque of size N+1)
            price_n_periods_ago = self._historical_prices[symbol][0]

            # Calculate momentum (Current Price - Price N periods ago)
            momentum = current_price - price_n_periods_ago
            logger.debug(f"MomentumStrategy: {symbol} at {timestamp} - Momentum ({self.momentum_period} period): {momentum:.2f} (Current: {current_price:.2f}, {self.momentum_period} periods ago: {price_n_periods_ago:.2f})")

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

        else:
            # Not enough history yet
            logger.debug(f"MomentumStrategy: {symbol} at {timestamp} - Not enough history for momentum calculation ({len(self._historical_prices[symbol])}/{self.momentum_period + 1}).")
