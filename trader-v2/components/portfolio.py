# components/portfolio.py
import logging
import asyncio # Import asyncio for potential async operations
from typing import Any, Dict
import uuid # Needed to generate OrderEvent ID

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import SignalEvent, OrderEvent, FillEvent # Import events portfolio interacts with

logger = logging.getLogger(__name__)

class BasePortfolio(BaseComponent):
    """
    Base class for Portfolio management.
    Handles position keeping, cash management, and generates orders from signals.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")
        # Initialize basic portfolio state (can be more complex in subclasses)
        self.cash = 100000.0 # Starting cash
        self.positions: Dict[str, Any] = {} # Example: {'AAPL': {'quantity': 100, 'cost_basis': 150.5}}
        self.pending_orders: Dict[str, OrderEvent] = {} # Track pending orders by ID

    def _setup_event_handlers(self):
        """Register portfolio's event handlers."""
        # Portfolio listens to SignalEvents to generate orders
        self.event_bus.subscribe(SignalEvent, self._on_signal_event)
        # Portfolio listens to FillEvents to update positions and cash
        self.event_bus.subscribe(FillEvent, self._on_fill_event)
        # Could also listen to OrderUpdateEvent in live trading

    async def _on_signal_event(self, event: SignalEvent):
        """Internal handler for SignalEvents. Calls the user-defined logic."""
        await self.on_signal(event) # Pass the signal to user logic

    async def _on_fill_event(self, event: FillEvent):
        """Internal handler for FillEvents. Calls the user-defined logic and updates state."""
        # Update portfolio state based on fill
        self._update_state_from_fill(event)
        # Call user-defined fill handling logic (e.g., for logging or performance tracking)
        await self.on_fill(event)

    def _update_state_from_fill(self, event: FillEvent):
        """Internal method to update cash and positions based on a fill."""
        symbol = event.symbol
        quantity = event.quantity
        price = event.price
        commission = event.commission
        direction = event.direction

        # Update cash
        if direction == "BUY":
            cost = quantity * price + commission
            self.cash -= cost
            logger.info(f"Portfolio: BUY fill for {quantity} of {symbol} at {price}. Cash reduced by {cost}. New cash: {self.cash}")
        elif direction == "SELL":
            proceeds = quantity * price - commission
            self.cash += proceeds
            logger.info(f"Portfolio: SELL fill for {quantity} of {symbol} at {price}. Cash increased by {proceeds}. New cash: {self.cash}")

        # Update positions
        if symbol not in self.positions:
            self.positions[symbol] = {'quantity': 0, 'cost_basis': 0.0}

        current_qty = self.positions[symbol]['quantity']
        current_cost_basis = self.positions[symbol]['cost_basis']

        if direction == "BUY":
            new_total_qty = current_qty + quantity
            new_total_cost = current_qty * current_cost_basis + quantity * price # Simple weighted average cost
            self.positions[symbol]['quantity'] = new_total_qty
            if new_total_qty > 0:
                 self.positions[symbol]['cost_basis'] = new_total_cost / new_total_qty
            else: # Should not happen on BUY, but for completeness
                 self.positions[symbol]['cost_basis'] = 0.0 # Or handle error
        elif direction == "SELL":
            # Assuming selling reduces quantity and cost basis proportionally (FIFO/LIFO would be different)
            new_total_qty = current_qty - quantity
            # For simplicity, if quantity sold <= current_qty, cost basis remains the same for remaining
            # If shorting is allowed, logic needs expansion. Assuming no shorting for now.
            if new_total_qty >= 0:
                 self.positions[symbol]['quantity'] = new_total_qty
                 # Cost basis of remaining shares is unchanged if we didn't go short
                 if new_total_qty == 0:
                     self.positions[symbol]['cost_basis'] = 0.0
            else:
                 logger.warning(f"Portfolio: Attempted to sell {quantity} of {symbol} but only {current_qty} held. Shorting not implemented.")
                 # Handle error or shorting logic
                 # For this simple example, we will just allow selling more than held
                 # In a real system, this would require more sophisticated handling or prevention
                 self.positions[symbol]['quantity'] = new_total_qty # Allows negative (short)


        logger.info(f"Portfolio: Updated position for {symbol}. New quantity: {self.positions[symbol]['quantity']}, Cost Basis: {self.positions[symbol]['cost_basis']:.2f}")

        # Remove from pending orders if applicable (matching event.order_id)
        if event.order_id in self.pending_orders:
             # Note: For partial fills, you might need to update pending order quantity
             # and only remove it on full fill. For simplicity, we remove on first fill for ID.
             del self.pending_orders[event.order_id]
             logger.debug(f"Portfolio: Removed pending order {event.order_id}")


    async def on_signal(self, signal_event: SignalEvent):
        """
        This method contains the portfolio's logic for handling signals
        and generating orders. Override this in subclasses.
        """
        # Example: print(f"Portfolio received signal for {signal_event.symbol}")
        pass # Subclasses must override

    async def on_fill(self, fill_event: FillEvent):
         """
         This method is called after portfolio state is updated from a fill.
         Override this in subclasses for custom fill handling (e.g., logging, P&L).
         """
         # Example: print(f"Portfolio processed fill for order {fill_event.order_id}")
         pass # Subclasses can override


# --- Example Portfolio Implementation ---

class ExamplePortfolio(BasePortfolio):
    """
    An example portfolio that places a market order for a fixed quantity
    when it receives a compatible signal, checking for sufficient cash.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized with ${self.cash:.2f} cash.")
        self._ordered_aapl = False # Simple flag to order only once for demo

    async def on_signal(self, signal_event: SignalEvent):
        """Handle signal event in example portfolio."""
        logger.debug(f"ExamplePortfolio received signal: {signal_event}")

        # Simple logic: If it's a LONG signal for AAPL and we haven't ordered yet
        if signal_event.symbol == 'AAPL' and signal_event.direction == 'LONG' and not self._ordered_aapl:
             # Determine quantity (simple fixed quantity for demo)
             order_quantity = 100
             order_symbol = signal_event.symbol
             order_direction = "BUY" # LONG signal -> BUY order

             # Basic cash check (assuming Market Order, price unknown)
             # This check is simplistic; real check would need price estimate + buffer
             # Let's assume average price is around 150, check if we have enough for 100 shares
             estimated_cost = order_quantity * 155.0 # Add a buffer
             if self.cash >= estimated_cost:
                 logger.info(f"ExamplePortfolio: Sufficient cash (${self.cash:.2f}) for BUY order of {order_quantity} {order_symbol}. Estimated cost: {estimated_cost:.2f}")
                 # Create and publish OrderEvent
                 order_event = OrderEvent(
                     symbol=order_symbol,
                     direction=order_direction,
                     quantity=order_quantity,
                     order_type="MARKET" # Simple market order
                 )
                 # Store pending order (optional but good practice for real system)
                 self.pending_orders[order_event.id] = order_event
                 logger.info(f"ExamplePortfolio: Publishing OrderEvent: {order_event}")
                 self.event_bus.publish(order_event)
                 self._ordered_aapl = True # Mark as ordered
             else:
                 logger.warning(f"ExamplePortfolio: Insufficient cash (${self.cash:.2f}) for BUY order of {order_quantity} {order_symbol}.")


    async def on_fill(self, fill_event: FillEvent):
         """Handle fill event in example portfolio (after state update)."""
         logger.info(f"ExamplePortfolio: Fill processed for order {fill_event.order_id}. Current Cash: ${self.cash:.2f}")
         # Here you could add more specific logging or trigger performance updates
         # The base class already updated self.cash and self.positions