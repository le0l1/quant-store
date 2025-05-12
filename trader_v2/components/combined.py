
# components/combined.py
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List
import uuid
import math
from collections import defaultdict

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import SignalEvent, OrderEvent, FillEvent, MarketEvent

logger = logging.getLogger(__name__)

# --- Execution Handler Part ---

class BaseExecutionHandler(BaseComponent):
    """
    Base class for Execution Handlers.
    Responsible for executing orders and publishing FillEvents.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        logger.info(f"{self.__class__.__name__} initialized.")

    def _setup_event_handlers(self):
        self.event_bus.subscribe(OrderEvent, self.on_order_event)
        self.event_bus.subscribe(MarketEvent, self.on_market_event)

    async def on_order_event(self, event: OrderEvent):
        logger.debug(f"BaseExecutionHandler received order {event.id}. Override _on_order_event in subclass.")
        pass
    
    async def on_market_event(self, event: MarketEvent):
        logger.debug(f"BaseExecutionHandler received market event {event.symbol} at {event.timestamp}. Override _on_market_event in subclass.")
        pass


class SimulatedExecutionHandler(BaseExecutionHandler):
    def __init__(self, 
        event_bus: EventBus, 
        commission_percent: float = 0.0, 
        slippage_percent: float = 0.0
    ):
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
    
    async def on_order_event(self, order_event: OrderEvent):
        logger.info('接受订单事件')
        self._pending_orders[order_event.id] = order_event

    async def on_market_event(self, market_event: MarketEvent):
        current_timestamp = market_event.timestamp
        # Update the last known price for this symbol regardless of whether it's a new timestep
        self._update_last_price_for_symbol(market_event.symbol, market_event.data)

        is_new_time_step = (self._last_market_timestamp is None or current_timestamp > self._last_market_timestamp)

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

            logger.info(f"SimulatedExecutionHandler: 撮合订单 {order.id} with FillEvent {fill_event.id}: {fill_event.direction} {fill_event.quantity} of {fill_event.symbol} at ${fill_event.price:.4f} (Commission: ${fill_event.commission:.4f}, using data at {current_timestamp})")

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



    # --- Portfolio Part ---

class BasePortfolio(BaseComponent):
    """
    Base class for Portfolio management.
    Handles position keeping, cash management, and generates orders from signals.
    Also tracks Net Asset Value (NAV) history.
    """
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0):
        super().__init__(event_bus)
        self.initial_cash = initial_cash
        self.cash = initial_cash # Cash available for trading
        self.positions: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'quantity': 0, 'cost_basis': 0.0}) # Current positions
        self.pending_orders: Dict[str, OrderEvent] = {} # {order_id: OrderEvent}
        self._last_market_prices: Dict[str, float] = {} # Store latest prices for estimations

        # NAV history storage (simple example)
        self.nav_history: Dict[datetime, float] = {}
        # Timestamp of the last recorded NAV for optimization
        self._last_nav_timestamp: Optional[datetime] = None

        logger.info(f"{self.__class__.__name__} initialized with ${self.initial_cash:.2f} cash.")

    def _setup_event_handlers(self):
        """Subscribe to necessary events."""
        self.event_bus.subscribe(SignalEvent, self._on_signal_event)
        self.event_bus.subscribe(FillEvent, self._on_fill_event)
        self.event_bus.subscribe(MarketEvent, self._on_market_event) # Used for price updates and potential NAV calculation

    async def _on_market_event(self, event: MarketEvent):
        """Handle market events - primarily for price updates and potential NAV calculation."""
        self._on_market_event_for_price(event)
        if hasattr(event, 'timestamp') and isinstance(event.timestamp, datetime):
             self._record_nav(event.timestamp)

    def _on_market_event_for_price(self, event: MarketEvent):
        """Update internal tracker for latest market prices."""
        # Use 'close' if available, otherwise 'price'
        price = event.data.get('close', event.data.get('price'))
        if price is not None and price > 0:
            self._last_market_prices[event.symbol] = price

    async def _on_signal_event(self, event: SignalEvent):
        """Entry point for signal processing. Calls the specific implementation."""
        logger.info(f"Portfolio received SignalEvent: {event}") # Keep this as it's a key input
        await self.on_signal(event)

    async def _on_fill_event(self, event: FillEvent):
        """Entry point for fill processing. Updates state then calls specific handler."""
        logger.info(f"Portfolio received FillEvent: {event}") # Keep this as it's a key event
        self._update_state_from_fill(event)
        await self.on_fill(event) # Allow subclasses to react after state is updated
        # Record NAV after state change due to fill
        if hasattr(event, 'timestamp') and isinstance(event.timestamp, datetime):
             self._record_nav(event.timestamp)

    # --- Helper Methods for Calculation and Checks ---
    def _calculate_commission(self, quantity: int, price: float) -> float:
        """Calculates estimated commission for an order."""
        return abs(quantity * price) * DEFAULT_COMMISSION_RATE

    def _estimate_order_cost(self, order: OrderEvent) -> float:
        if order.quantity <= 0: return 0.0

        symbol = order.symbol
        quantity = order.quantity
        direction = order.direction

        estimated_price = order.price # Use limit price if available
        if order.order_type == 'MARKET' or estimated_price is None:
            last_price = self.get_latest_price(symbol)
            if last_price is None:
                logger.warning(f"Portfolio Estimate: Cannot estimate cost for MARKET order {symbol}, no recent market price.")
                return 0.0 # Cannot estimate without price

            # Apply slippage only for BUY market orders impacting available cash
            if direction == "BUY":
                 estimated_price = last_price * (1 + DEFAULT_SLIPPAGE_PERCENT)
            else: # SELL or other types, use last price for estimation
                 estimated_price = last_price

        if estimated_price <= 0:
            logger.warning(f"Portfolio Estimate: Estimated price for {symbol} is non-positive ({estimated_price:.2f}). Cannot accurately estimate cost.")
            return 0.0 # Cannot estimate with non-positive price

        estimated_value = quantity * estimated_price
        commission = self._calculate_commission(quantity, estimated_price)

        if direction == "BUY":
             # Estimated cash needed = value + commission
            return estimated_value + commission
        elif direction == "SELL":
            return estimated_value - commission # Estimated proceeds
        else:
            return 0.0 # Should not happen


    def _check_risk_before_order(self, potential_order: OrderEvent) -> bool:
        """
        Checks if the potential order is permissible given current CONFIRMED state.
        Returns True if the order can be placed, False otherwise.
        """
        symbol = potential_order.symbol
        direction = potential_order.direction
        quantity = potential_order.quantity

        if quantity <= 0:
            logger.warning(f"RISK CHECK: Blocking order for {symbol} with non-positive quantity ({quantity}).")
            return False

        # --- Cash Check (Primarily for BUYs) ---
        if direction == "BUY":
            estimated_cost = self._estimate_order_cost(potential_order)
            if estimated_cost <= 0:
                 # This case is already logged within _estimate_order_cost
                logger.warning(f"RISK CHECK: Blocking BUY order for {symbol} due to inability to estimate cost.")
                return False

            # Check against available cash
            if estimated_cost > self.cash:
                logger.warning(f"RISK CHECK FAILED: Insufficient cash for BUY {quantity} {symbol}. "
                              f"Required: ~${estimated_cost:.2f}, Available: ${self.cash:.2f}")
                return False

        allow_shorting = False # Example flag: Set to True if your strategy allows shorting
        if not allow_shorting and direction == "SELL":
            confirmed_qty = self.get_current_position_quantity(symbol)
            if quantity > confirmed_qty:
                logger.warning(f"RISK CHECK FAILED: Cannot SELL {quantity} {symbol}. "
                              f"Current position is {confirmed_qty}. "
                              f"Selling more than confirmed position (Shorting not allowed).")
                return False

        logger.debug(f"RISK CHECK PASSED for order: {potential_order}") # Keep a debug log for successful checks

        return True # Passed all checks

    # --- State Update from Fill ---

    def _update_state_from_fill(self, event: FillEvent):
        """
        Updates portfolio state (cash, positions, pending orders) based on a FillEvent.
        Includes checks to ensure state consistency (sufficient cash for BUY, sufficient position for SELL if shorting disallowed).
        """
        order_id = event.order_id
        symbol = event.symbol
        quantity_filled = event.quantity
        price = event.price
        commission = event.commission
        direction = event.direction
        fill_time = event.timestamp

        # Ensure quantities and prices are valid for processing
        if quantity_filled <= 0 or price <= 0:
             logger.error(f"Portfolio ERROR: Received FillEvent with invalid quantity ({quantity_filled}) or price ({price}) for order_id '{order_id}'. State update blocked.")
             return

        logger.debug(f"Portfolio processing FillEvent: {event}")

        # --- 1. Remove the order from pending if it exists ---
        original_order = self.pending_orders.get(order_id)
        if original_order:
            # Clean up pending orders
            del self.pending_orders[order_id]
            logger.debug(f"Portfolio: Removed pending order {order_id}. {len(self.pending_orders)} pending orders remain.")

        else:
            # This happens for subsequent fills of the same order_id if we remove it on the first fill,
            # or if a fill event arrives for an order we never tracked as pending (e.g., from manual trades).
            logger.warning(f"Portfolio: Received FillEvent for order_id '{order_id}' which was not found in pending orders. Assuming external fill or subsequent fill for a processed order. State might be inconsistent if not expected.")
            # Proceed to update confirmed state based on the fill, but be aware.


        # --- 2. Update Confirmed State using ACTUAL Fill Data ---
        current_qty = self.get_current_position_quantity(symbol)

        if direction == "BUY":
            cost = (quantity_filled * price) + commission
            # --- EXPLICIT CHECK: Sufficient Cash for BUY Fill ---
            if cost > self.cash:
                # This indicates an inconsistency between simulation/exchange and portfolio state
                logger.error(f"Portfolio ERROR: Insufficient CONFIRMED cash for BUY fill. Required: ${cost:.2f}, Available: ${self.cash:.2f}. Fill event: {event}. State update BLOCKED.")
                # DO NOT update cash or position if the fill would result in negative confirmed cash
                return
            # --- End Check ---

            self.cash -= cost
            logger.info(f"Portfolio: CONFIRMED BUY fill for {quantity_filled} of {symbol} at ${price:.4f} (Commission: ${commission:.4f}). Cash reduced by ${cost:.2f}. New cash: ${self.cash:.2f}")

            # Update position quantity and cost basis (using average cost)
            new_total_qty = current_qty + quantity_filled
            if new_total_qty > 0:
                # Calculate weighted average cost basis
                current_cost_value = current_qty * self.positions[symbol]['cost_basis']
                fill_cost_value = quantity_filled * price # Use fill price for cost basis contribution
                new_total_cost_value = current_cost_value + fill_cost_value
                new_cost_basis = new_total_cost_value / new_total_qty
                self.positions[symbol]['quantity'] = new_total_qty
                self.positions[symbol]['cost_basis'] = new_cost_basis
                logger.debug(f"Portfolio: Updated {symbol} position. New quantity: {new_total_qty}, Avg Cost: ${new_cost_basis:.4f}")
            elif new_total_qty == 0:
                 # Closed a short position
                 self.positions[symbol]['quantity'] = 0
                 self.positions[symbol]['cost_basis'] = 0.0 # Reset cost basis when flat
                 logger.info(f"Portfolio: Closed short position for {symbol} via BUY fill. Position is now 0.")
            else: # new_total_qty < 0 (Should only happen if starting from a short position and buying doesn't fully cover)
                 self.positions[symbol]['quantity'] = new_total_qty
                 # Cost basis tracking for short positions is more complex (e.g., proceeds from shorting).
                 # This simple example resets cost basis or leaves it from prior long positions.
                 # A more robust system would track short positions separately or handle cost basis for shorting.
                 logger.warning(f"Portfolio: BUY fill resulted in a less-negative (or more-negative) short position for {symbol}. Cost basis tracking for shorts not fully implemented.")


        elif direction == "SELL":
            if quantity_filled > current_qty:
                quantity_filled = current_qty
                
            proceeds = (quantity_filled * price) - commission
            self.cash += proceeds
            logger.info(f"Portfolio: CONFIRMED SELL fill for {quantity_filled} of {symbol} at ${price:.4f} (Commission: ${commission:.4f}). Cash increased by ${proceeds:.2f}. New cash: ${self.cash:.2f}")

            # Update position quantity and handle cost basis (for liquidating long positions)
            new_total_qty = current_qty - quantity_filled
            self.positions[symbol]['quantity'] = new_total_qty

            if new_total_qty == 0:
                 self.positions[symbol]['cost_basis'] = 0.0 # Reset cost basis when flat
                 logger.info(f"Portfolio: Closed long position for {symbol} via SELL fill. Position is now 0.")
            elif new_total_qty < 0:
                 self.positions[symbol]['cost_basis'] = 0.0 # Or some indicator of short basis
                 logger.warning(f"Portfolio: SELL fill resulted in a short position for {symbol}. Cost basis tracking for shorts not fully implemented.")

        else:
             logger.warning(f"Portfolio: Received FillEvent with unhandled direction '{direction}' for order_id '{order_id}'. State update skipped.")


        if self.positions[symbol]['quantity'] == 0 and self.positions[symbol]['cost_basis'] == 0.0:
            del self.positions[symbol]
            logger.debug(f"Portfolio: Removed zero position entry for {symbol}.")


    # --- Accessor Methods ---

    def get_total_portfolio_value(self) -> float:
        """Calculates the total market value of the portfolio (Cash + Holdings)."""
        total_value = self.cash
        for symbol, pos_info in self.positions.items():
            quantity = pos_info['quantity']
            if quantity != 0:
                latest_price = self._last_market_prices.get(symbol)
                if latest_price is not None and latest_price > 0:
                    total_value += quantity * latest_price
                else:
                    # Fallback: If no latest price, use cost basis value.
                    # Note: This is an approximation and less accurate than market value.
                    logger.warning(f"Portfolio Value: No valid latest price for {symbol} ({latest_price}) to calculate market value. Using cost basis estimate: {quantity * pos_info['cost_basis']:.2f}")
                    total_value += quantity * pos_info['cost_basis'] # Fallback estimate

        return total_value

    def get_current_position_quantity(self, symbol: str) -> int:
        """Gets the confirmed position quantity for a symbol."""
        return self.positions.get(symbol, {}).get('quantity', 0)

    def get_available_cash(self) -> float:
        """Gets the cash available for new trades."""
        return max(0.0, self.cash)  # Now simply returns the available cash without considering pending orders

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Gets the last known market price for a symbol."""
        price = self._last_market_prices.get(symbol)
        if price is not None and price > 0:
            return price
        return None

    def _record_nav(self, timestamp: datetime):
        """Records the Net Asset Value (NAV) at a specific timestamp."""
        # Avoid recording NAV multiple times for the exact same timestamp if events arrive rapidly
        if self._last_nav_timestamp and timestamp <= self._last_nav_timestamp:
            return # Already recorded NAV for this time or a later time

        total_value = self.get_total_portfolio_value()
        self.nav_history[timestamp] = total_value
        self._last_nav_timestamp = timestamp

    # --- Abstract Methods for Subclasses ---

    async def on_signal(self, signal_event: SignalEvent):
        """Subclasses must implement specific logic to handle signals."""
        raise NotImplementedError("Subclasses must implement on_signal")

    async def on_fill(self, fill_event: FillEvent):
        """Optional: Subclasses can implement logic after a fill affects state."""
        pass


# --- Momentum Portfolio Implementation ---

class MomentumPortfolio(BasePortfolio):
    """
    A portfolio implementation for a momentum strategy.
    Calculates order quantity based on signal weight (percentage of total portfolio value)
    and adheres to lot size constraints.
    """
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0, lot_size: int = 1):
        super().__init__(event_bus, initial_cash)
        self.lot_size = max(1, int(lot_size)) # Ensure lot size is at least 1
        logger.info(f"{self.__class__.__name__} initialized with lot_size={self.lot_size}.")

    async def on_signal(self, signal_event: SignalEvent):
        logger.info(f"MomentumPortfolio processing signal: {signal_event}") # More specific log
        symbol = signal_event.symbol
        direction = signal_event.direction # Expected: LONG, SHORT, FLAT
        weight = signal_event.weight      # Expected: Target weight (e.g., 0.1 for 10%)
        signal_time = signal_event.timestamp

        # --- 1. Handle FLAT Signal to Close Position ---
        if direction == "FLAT":
            confirmed_qty = self.get_current_position_quantity(symbol)

            # If already flat, do nothing
            if confirmed_qty == 0:
                logger.debug(f"MomentumPortfolio: Received FLAT signal for {symbol}, but already flat.")
                return

            # Determine quantity needed to get to zero confirmed position
            # If confirmed_qty is positive, we need to SELL that amount.
            # If confirmed_qty is negative (short), we need to BUY that amount to cover.
            quantity_to_flatten = abs(confirmed_qty) # Quantity to trade to reach 0 confirmed position

            if quantity_to_flatten > 0:
                 await self._generate_flat_order(symbol, quantity_to_flatten, signal_time)
            else:
                 logger.debug(f"MomentumPortfolio: Received FLAT signal for {symbol}, confirmed position is 0. No order needed.")

            return # FLAT signal processed

        # --- 2. Handle LONG/SHORT Signals with Target Weight ---
        if direction not in ["LONG", "SHORT"] or weight is None or weight < 0:
            logger.warning(f"MomentumPortfolio: Received invalid or unsupported signal direction '{direction}' or weight '{weight}' for {symbol}. No order generated.")
            return

        # --- Get Necessary Data ---
        latest_price = self.get_latest_price(symbol)
        if latest_price is None or latest_price <= 0:
            logger.warning(f"MomentumPortfolio: Cannot get valid latest price for {symbol} ({latest_price}). Cannot process signal.")
            return

        total_portfolio_value = self.get_total_portfolio_value()
        if total_portfolio_value <= 1e-6: # Use a small epsilon to check for near zero
            logger.warning(f"MomentumPortfolio: Total portfolio value is near zero or negative (${total_portfolio_value:.2f}). Cannot calculate target quantity.")
            return

        # --- 3. Calculate Target vs Current Position ---
        # Target value based on portfolio equity and signal weight
        target_value = total_portfolio_value * weight
        target_value = max(0.0, target_value) # Ensure target value is non-negative

        # Calculate target quantity (float) based on target value and current price
        # Prevent division by zero if price is somehow zero
        target_quantity_float = target_value / latest_price if latest_price > 1e-9 else 0.0

        # Get the current projected quantity
        current_quantity = self.get_current_position_quantity(symbol)

        # Determine the desired target quantity sign based on signal direction
        # LONG target is positive, SHORT target is negative
        adjusted_target_quantity_float = target_quantity_float
        if direction == "SHORT":
            adjusted_target_quantity_float = -target_quantity_float

        # Calculate the difference between the desired target quantity and the current quantity
        # This difference is how much we need to trade (net) to reach the target.
        quantity_difference_float = adjusted_target_quantity_float - current_quantity

        logger.debug(f"MomentumPortfolio Calc: {symbol} | Sig Dir: {direction}, Weight: {weight:.2%} | Total Value: ${total_portfolio_value:.2f} | Target Value: ${target_value:.2f} | Target Qty (float): {target_quantity_float:.2f} | Adj Target Qty (float): {adjusted_target_quantity_float:.2f} | Current Qty: {current_quantity} | Qty Diff vs Current: {quantity_difference_float:.2f}")


        # --- 4. Determine Order Quantity and Direction (Apply Lot Size) ---
        order_quantity_int = 0
        order_direction: Optional[str] = None

        # If the difference is large enough (at least one lot) and positive, we need to BUY
        if quantity_difference_float > self.lot_size * 0.99: # Use a small tolerance for float comparison
            order_direction = "BUY"
            # Calculate discrete lots to buy, rounding down
            order_quantity_int = int(math.floor(quantity_difference_float / self.lot_size) * self.lot_size)

        # If the difference is large enough (at least one lot) and negative, we need to SELL
        elif quantity_difference_float < -self.lot_size * 0.99:
            order_direction = "SELL"
            # Calculate discrete lots to sell, rounding down the absolute difference
            order_quantity_int = int(math.floor(abs(quantity_difference_float) / self.lot_size) * self.lot_size)

        else:
            # The difference is less than a full lot, no trade needed to adjust to target
            logger.debug(f"MomentumPortfolio: {symbol} | Quantity difference vs current ({quantity_difference_float:.2f}) is less than lot size ({self.lot_size}). No order needed.")
            return # No order to generate

        # --- 5. Create and Place Order if Quantity is Positive ---
        if order_quantity_int <= 0 or order_direction is None:
            logger.debug(f"MomentumPortfolio: Calculated order quantity is zero or negative ({order_quantity_int}). No order generated.")
            return # Should not happen if logic above is correct, but good defensive check

        # Construct the potential order event
        potential_order = OrderEvent(
            symbol=symbol,
            direction=order_direction,
            quantity=order_quantity_int,
            order_type="MARKET" # Assuming MARKET orders for simplicity
            # price=None # MARKET orders don't have a specific price here
        )

        # Generate a unique ID for the order
        order_id = str(uuid.uuid4())
        order_timestamp = signal_time

        # Create the final OrderEvent to publish
        order_event = OrderEvent(
            id=order_id,
            timestamp=order_timestamp,
            symbol=potential_order.symbol,
            direction=potential_order.direction,
            quantity=potential_order.quantity,
            order_type=potential_order.order_type,
            price=None # Market order
        )

        # --- Risk Check ---
        # Check if placing this order is permissible given current state
        if self._check_risk_before_order(order_event):
            # Add the order to the pending queue
            self.pending_orders[order_event.id] = order_event
            logger.debug(f"MomentumPortfolio: Added order {order_event.id} to pending queue.")

            # Publish the order event to the event bus for the execution handler
            logger.info(f"MomentumPortfolio: Publishing OrderEvent: {order_event}")
            self.event_bus.publish(order_event)
        else:
            # Order was blocked by the risk check
            logger.warning(f"MomentumPortfolio: Order generation for {potential_order} blocked by risk check.")


    async def _generate_flat_order(self, symbol: str, quantity: int, signal_time: datetime):
        """
        Generates an order to close a specific quantity of the current CONFIRMED position for a symbol.
        Used internally for FLAT signals.
        """
        current_quantity = self.get_current_position_quantity(symbol)

        if current_quantity == 0 or quantity <= 0:
            logger.debug(f"MomentumPortfolio: Not generating FLAT order for {symbol} as current confirmed quantity is {current_quantity} or requested quantity is {quantity}.")
            return

        # Determine direction to flatten the position
        order_direction = "SELL" if current_quantity > 0 else "BUY"
        # The quantity to order is the absolute value of the confirmed quantity we want to close
        order_quantity_int = quantity # Use the quantity passed in, which should be abs(confirmed_qty)

        logger.info(f"MomentumPortfolio: Generating FLAT order for {symbol} to close {order_quantity_int} shares (Direction: {order_direction}).")

        # Create the potential order event
        potential_order = OrderEvent(
             symbol=symbol,
             direction=order_direction,
             quantity=order_quantity_int,
             order_type="MARKET" # Assuming MARKET orders for simplicity
             # price=None
        )

        # --- Risk Check ---
        # Check if placing this order is permissible
        if self._check_risk_before_order(potential_order):
            order_id = str(uuid.uuid4())

            # Create the final OrderEvent to publish
            order_event = OrderEvent(
                 id=order_id,
                 timestamp=signal_time,
                 symbol=potential_order.symbol,
                 direction=potential_order.direction,
                 quantity=potential_order.quantity,
                 order_type=potential_order.order_type,
                 price=None # Market order
             )

            # Add the order to the pending queue
            self.pending_orders[order_event.id] = order_event
            logger.debug(f"MomentumPortfolio: Added FLAT order {order_event.id} to pending queue.")

            # Publish the order event
            logger.info(f"MomentumPortfolio: Publishing FLAT OrderEvent: {order_event}")
            self.event_bus.publish(order_event)
        else:
            # Order was blocked by the risk check
            logger.warning(f"MomentumPortfolio: FLAT Order generation for {potential_order} blocked by risk check.")


    async def on_fill(self, fill_event: FillEvent):
        """Log portfolio summary after a fill updates the state."""
        # After the fill updates the state in the base class, log a summary.
        # You could add more complex logic here if needed for the Momentum strategy.
        logger.info(f"MomentumPortfolio Post-Fill Summary: Cash: ${self.cash:.2f}, Total Value: ${self.get_total_portfolio_value():.2f}")
        # Log positions if any exist and are non-zero
        non_zero_positions = {sym: pos['quantity'] for sym, pos in self.positions.items() if pos['quantity'] != 0}
        if non_zero_positions:
            logger.info(f"MomentumPortfolio Positions: {non_zero_positions}")
        else:
             logger.info("MomentumPortfolio Positions: No open positions.")

        # Log pending orders for visibility
        if self.pending_orders:
            logger.info(f"MomentumPortfolio Pending Orders ({len(self.pending_orders)}): {[f'{order.direction} {order.quantity} {order.symbol}' for order in self.pending_orders.values()]}")
        else:
             logger.info("MomentumPortfolio Pending Orders: None.")