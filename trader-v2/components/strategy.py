# components/strategy.py
import logging
import asyncio # Import asyncio for potential async operations
from typing import Any, Dict

from components.base import BaseComponent
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
    def __init__(self, event_bus: EventBus, momentum_period: int = 20, default_weight: float = 0.10):
        """
        Args:
            event_bus: The central Event Bus instance.
            momentum_period: The lookback period for momentum calculation.
            default_weight: The suggested allocation percentage for LONG/SHORT signals.
        """
        super().__init__(event_bus)
        self.momentum_period = momentum_period
        self.default_weight = default_weight
        # Dictionary to store historical closing prices for each symbol
        # Using deque with maxlen to automatically handle window size
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

            if momentum > 0:
                direction = "LONG"
                weight = self.default_weight # Suggest default_weight for LONG
                logger.debug(f"MomentumStrategy: {symbol} at {timestamp} - Positive momentum. Generating LONG signal with weight {weight}.")
                signal_event = SignalEvent(symbol=symbol, direction=direction, weight=weight)
                self.event_bus.publish(signal_event)

            elif momentum < 0:
                direction = "SHORT"
                weight = self.default_weight # Suggest default_weight for SHORT
                logger.info(f"MomentumStrategy: {symbol} at {timestamp} - Negative momentum. Generating SHORT signal with weight {weight}.")
                signal_event = SignalEvent(symbol=symbol, direction=direction, weight=weight)
                self.event_bus.publish(signal_event)

            else: # momentum == 0
                direction = "FLAT"
                # Optionally publish a FLAT signal, maybe with weight 0 or None
                logger.debug(f"MomentumStrategy: {symbol} at {timestamp} - Zero momentum. No signal generated (or would generate FLAT).")
                # signal_event = SignalEvent(symbol=symbol, direction=direction, weight=0.0) # Or weight=None
                # self.event_bus.publish(signal_event)

        else:
            # Not enough history yet
            logger.debug(f"MomentumStrategy: {symbol} at {timestamp} - Not enough history for momentum calculation ({len(self._historical_prices[symbol])}/{self.momentum_period + 1}).")
