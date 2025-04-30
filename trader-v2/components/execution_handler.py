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
    # ... (BaseExecutionHandler class code remains the same) ...
    """
    Base class for Execution Handlers.
    Responsible for executing orders and publishing FillEvents.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")

    def _setup_event_handlers(self):
        self.event_bus.subscribe(OrderEvent, self._on_order_event)

    async def _on_order_event(self, event: OrderEvent):
        await self.execute_order(event)

    async def execute_order(self, order_event: OrderEvent):
        logger.debug(f"BaseExecutionHandler received order {order_event.id}. Override execute_order in subclass.")
        pass


class SimulatedExecutionHandler(BaseExecutionHandler):
    def __init__(self, event_bus: EventBus, commission_percent: float = 0.0, slippage_percent: float = 0.0):
        """
        Args:
            event_bus: The central Event Bus instance.
            commission_percent: Commission percentage per trade (e.g., 0.001 for 0.1%).
            slippage_percent: Slippage percentage per trade (e.g., 0.0005 for 0.05%).
        """
        super().__init__(event_bus)
        self._pending_orders: Dict[str, OrderEvent] = {}
        self._last_market_timestamp: Optional[datetime] = None
        self._last_market_prices: Dict[str, float] = {} # Store latest market prices for potential fill price reference

        self.commission_percent = commission_percent # Store commission rate
        self.slippage_percent = slippage_percent     # Store slippage rate

        logger.info(f"{self.__class__.__name__} initialized with commission={self.commission_percent:.4f}, slippage={self.slippage_percent:.4f}.")
    # -------------------------------------------------------------


    def _setup_event_handlers(self):
        super()._setup_event_handlers()
        self.event_bus.subscribe(MarketEvent, self._on_market_event_for_settlement)


    async def _on_order_event(self, order_event: OrderEvent):
        logger.debug(f"SimulatedExecutionHandler received OrderEvent {order_event.id}. Adding to pending list.")
        self._pending_orders[order_event.id] = order_event
        # logger.info(f"SimulatedExecutionHandler: Order {order_event.id} added to pending list ({len(self._pending_orders)} pending).")
        await self.execute_order(order_event)


    async def execute_order(self, order_event: OrderEvent):
         logger.debug(f"SimulatedExecutionHandler executing (adding to pending) Order: {order_event.id}")
         pass


    async def _on_market_event_for_settlement(self, market_event: MarketEvent):
        current_timestamp = market_event.timestamp
        # Update the last known price for this symbol regardless of whether it's a new timestep
        self._update_last_price_for_symbol(market_event.symbol, market_event.data)

        is_new_time_step = (self._last_market_timestamp is None or current_timestamp > self._last_market_timestamp)

        # Store current timestamp for the next step *before* checking settlements for the *previous* step
        # self._last_market_timestamp = current_timestamp # Moved update to end


        if not is_new_time_step:
            logger.debug(f"SimulatedExecutionHandler: Still processing data for timestamp {current_timestamp}. No settlement triggered yet.")
            return

        # --- New Time Step - Settle Orders ---
        orders_to_settle_ids = list(self._pending_orders.keys())

        for order_id in orders_to_settle_ids:
            order = self._pending_orders.get(order_id)
            if not order:
                 logger.warning(f"SimulatedExecutionHandler: Pending order {order_id} not found during settlement check.")
                 continue

            # Determine base simulated fill price using the NEW market data
            # For simplicity, use the 'open' price of the new bar for MARKET orders
            base_simulated_price = None
            if 'open' in market_event.data:
                 base_simulated_price = market_event.data['open']
            elif 'close' in market_event.data: # Fallback
                 base_simulated_price = market_event.data['close']
            # Add logic for LIMIT/STOP orders checking price levels

            if base_simulated_price is None or base_simulated_price <= 0:
                 logger.warning(f"SimulatedExecutionHandler: Cannot determine valid settlement price for {order.symbol} at {current_timestamp} ({base_simulated_price}). Order {order.id} remains pending.")
                 continue # Order remains pending

            # --- Apply Slippage ---
            final_fill_price = base_simulated_price
            slippage_amount = base_simulated_price * self.slippage_percent

            if order.direction == "BUY":
                # Slippage increases buy price
                final_fill_price = base_simulated_price + slippage_amount
                logger.debug(f"SimulatedExecutionHandler: Applied BUY slippage ({self.slippage_percent:.4f}) of ${slippage_amount:.4f} to {order.id}. Base price ${base_simulated_price:.4f} -> Final price ${final_fill_price:.4f}")
            elif order.direction == "SELL":
                # Slippage decreases sell price
                final_fill_price = base_simulated_price - slippage_amount
                logger.debug(f"SimulatedExecutionHandler: Applied SELL slippage ({self.slippage_percent:.4f}) of ${slippage_amount:.4f} to {order.id}. Base price ${base_simulated_price:.4f} -> Final price ${final_fill_price:.4f}")

            # Ensure final price is positive after slippage (shouldn't be an issue with typical market data)
            if final_fill_price <= 0:
                 logger.warning(f"SimulatedExecutionHandler: Final fill price for {order.symbol} is zero or negative ({final_fill_price}) after slippage. Order {order.id} remains pending.")
                 continue


            # --- Calculate Commission ---
            # Commission is based on the final fill price and quantity
            simulated_commission = final_fill_price * order.quantity * self.commission_percent
            logger.debug(f"SimulatedExecutionHandler: Calculated commission ({self.commission_percent:.4f}) of ${simulated_commission:.4f} for order {order.id} (Value: ${final_fill_price * order.quantity:.4f})")


            # Create FillEvent
            fill_event = FillEvent(
                order_id=order.id,
                symbol=order.symbol,
                direction=order.direction,
                quantity=order.quantity, # Assume full fill for simplicity
                price=final_fill_price,  # Use the final price after slippage
                commission=simulated_commission, # Use the calculated commission
                timestamp=current_timestamp
            )

            logger.info(f"SimulatedExecutionHandler: Settled Order {order.id} with FillEvent {fill_event.id}: {fill_event.direction} {fill_event.quantity} of {fill_event.symbol} at ${fill_event.price:.4f} (Commission: ${fill_event.commission:.4f}, using data at {current_timestamp})")

            # Publish FillEvent
            self.event_bus.publish(fill_event)

            # Remove order from pending list
            if order_id in self._pending_orders:
                del self._pending_orders[order_id]
                logger.debug(f"SimulatedExecutionHandler: Order {order_id} removed from pending list.")
            else:
                 logger.warning(f"SimulatedExecutionHandler: Order {order_id} was already removed from pending list?")


        # Update the last market timestamp to the current timestamp *after* processing settlements
        self._last_market_timestamp = current_timestamp


    def _update_last_price_for_symbol(self, symbol: str, data: Dict[str, Any]):
        # ... (This method remains the same) ...
        """Helper to store latest prices."""
        price = None
        if 'close' in data:
             price = data['close']
        elif 'price' in data: # For tick data potentially
             price = data['price']
        elif 'open' in data: # Might store open too
             price = data['open']

        if price is not None and price > 0: # Ensure price is valid and positive
            self._last_market_prices[symbol] = price