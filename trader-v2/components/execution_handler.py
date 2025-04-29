# components/execution_handler.py
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, Any
import uuid

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import OrderEvent, FillEvent, MarketEvent

logger = logging.getLogger(__name__)

class BaseExecutionHandler(BaseComponent):
    """
    Base class for Execution Handlers.
    Responsible for executing orders and publishing FillEvents.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")

    def _setup_event_handlers(self):
        """Register execution handler's event handlers."""
        self.event_bus.subscribe(OrderEvent, self._on_order_event)

    async def _on_order_event(self, event: OrderEvent):
        """Internal handler for OrderEvents. Calls the user-defined execution logic."""
        await self.execute_order(event)

    async def execute_order(self, order_event: OrderEvent):
        """
        This method contains the logic for executing orders.
        Override this in subclasses (Simulated vs. Broker).
        """
        logger.debug(f"BaseExecutionHandler received order {order_event.id}. Override execute_order in subclass.")
        pass # Subclasses must override


# --- Simulated Execution Handler for Backtesting ---

class SimulatedExecutionHandler(BaseExecutionHandler):
    """
    A simulated execution handler for backtesting.
    It receives OrderEvents and simulates fills based on subsequent MarketEvents.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        self._pending_orders: Dict[str, OrderEvent] = {}
        self._last_market_timestamp: Optional[datetime] = None
        # --- FIX: Initialize _last_market_prices ---
        self._last_market_prices: Dict[str, float] = {}
        # --------------------------------------------
        logger.info(f"{self.__class__.__name__} initialized.")


    def _setup_event_handlers(self):
        """Register simulated execution handler's event handlers."""
        super()._setup_event_handlers()

        self.event_bus.subscribe(MarketEvent, self._on_market_event_for_settlement)


    async def _on_order_event(self, order_event: OrderEvent):
        """Handles incoming OrderEvents by adding them to pending orders."""
        logger.debug(f"SimulatedExecutionHandler received OrderEvent {order_event.id} for {order_event.direction} {order_event.quantity} of {order_event.symbol}")
        # Store the order. It will be processed when the next relevant MarketEvent arrives.
        self._pending_orders[order_event.id] = order_event
        logger.info(f"SimulatedExecutionHandler: Order {order_event.id} added to pending list ({len(self._pending_orders)} pending).")
        # Note: The base class calls execute_order after receiving the event.
        # We keep it and use it to add to pending state.
        await self.execute_order(order_event)


    async def execute_order(self, order_event: OrderEvent):
         """Adds the order to the pending list for simulation."""
         # This method is called by _on_order_event.
         # The actual simulation happens when _on_market_event_for_settlement is called.
         logger.debug(f"SimulatedExecutionHandler executing (adding to pending) Order: {order_event.id}")
         # The adding is already done in _on_order_event, nothing more needed here for now.
         pass


    async def _on_market_event_for_settlement(self, market_event: MarketEvent):
        current_timestamp = market_event.timestamp
        logger.debug(f"SimulatedExecutionHandler received MarketEvent for {market_event.symbol} at {current_timestamp} to check for settlements.")

        self._update_last_price_for_symbol(market_event.symbol, market_event.data)

        is_new_time_step = (self._last_market_timestamp is None or current_timestamp > self._last_market_timestamp)



        if not is_new_time_step:
            logger.debug(f"SimulatedExecutionHandler: Still processing data for timestamp {current_timestamp}. No settlement triggered yet.")
            return # Wait for the next time step to settle orders from the *previous* step

        # --- New Time Step - Settle Orders ---
        logger.info(f"SimulatedExecutionHandler: New time step arrived: {current_timestamp}. Checking pending orders for settlement.")

        # IMPORTANT: Get keys before modifying the dictionary during iteration
        orders_to_settle_ids = list(self._pending_orders.keys())

        # Iterate through pending orders and simulate fills using the NEW market data
        for order_id in orders_to_settle_ids:
            order = self._pending_orders.get(order_id)
            if not order:
                 logger.warning(f"SimulatedExecutionHandler: Pending order {order_id} not found during settlement check.")
                 continue

            simulated_fill_price = None
            if 'open' in market_event.data:
                 simulated_fill_price = market_event.data['open']
            elif 'close' in market_event.data:
                 simulated_fill_price = market_event.data['close']

            if simulated_fill_price is None:
                 logger.warning(f"SimulatedExecutionHandler: Cannot determine settlement price for {order.symbol} at {current_timestamp}. Order {order.id} remains pending.")
                 continue

            # Calculate simulated commission (example: fixed percentage)
            simulated_commission = order.quantity * simulated_fill_price * 0.001 # 0.1% commission example

            # Create FillEvent
            fill_event = FillEvent(
                order_id=order.id,
                symbol=order.symbol,
                direction=order.direction,
                quantity=order.quantity, # Assume full fill for simplicity
                price=simulated_fill_price,
                commission=simulated_commission
            )

            logger.info(f"SimulatedExecutionHandler: Settled Order {order.id} with FillEvent {fill_event.id}: {fill_event.direction} {fill_event.quantity} of {fill_event.symbol} at {fill_event.price} (using data at {current_timestamp})")

            # Publish FillEvent
            self.event_bus.publish(fill_event)

            # Remove order from pending list
            if order_id in self._pending_orders: # Check exists before deleting
                del self._pending_orders[order_id]
                logger.debug(f"SimulatedExecutionHandler: Order {order_id} removed from pending list.")
            else:
                 logger.warning(f"SimulatedExecutionHandler: Order {order_id} was already removed from pending list?")


        # After processing settlements for the previous step using the current data,
        # update the last market timestamp to the current timestamp.
        # This happens *after* the loop.
        self._last_market_timestamp = current_timestamp
        logger.info(f"SimulatedExecutionHandler: Settlement check for {current_timestamp} complete. {len(self._pending_orders)} orders still pending. Updated last market timestamp to {self._last_market_timestamp}.")


    def _update_last_price_for_symbol(self, symbol: str, data: Dict[str, Any]):
        """Helper to store latest prices."""
        # This method is called for *every* MarketEvent, regardless of time step,
        # to keep the most recent price updated.
        price = None
        if 'close' in data:
             price = data['close']
        elif 'price' in data: # For tick data potentially
             price = data['price']
        elif 'open' in data: # Might store open too
             price = data['open']

        if price is not None:
            self._last_market_prices[symbol] = price
            # logger.debug(f"SimulatedExecutionHandler updated last price for {symbol}: {self._last_market_prices[symbol]}")