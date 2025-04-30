# components/portfolio.py
import logging
import asyncio
from datetime import datetime
from typing import Any, Dict, Optional
import uuid
import math
from collections import defaultdict

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import SignalEvent, OrderEvent, FillEvent, MarketEvent # Assume these dataclasses exist

logger = logging.getLogger(__name__)

# --- Configuration Constants ---
DEFAULT_COMMISSION_RATE = 0.0005 # Example: 0.05%
DEFAULT_SLIPPAGE_PERCENT = 0.001 # Example: 0.1% for market order estimations

class BasePortfolio(BaseComponent):
    """
    Base class for Portfolio management.
    Handles position keeping, cash management, and generates orders from signals.
    Includes pending order tracking and pre-deduction for robust backtesting.
    """
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0):
        super().__init__(event_bus)
        self.initial_cash = initial_cash
        self.cash = initial_cash # Confirmed cash
        self.positions: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'quantity': 0, 'cost_basis': 0.0}) # Confirmed positions

        # --- State for Pending Orders and Pre-deduction ---
        self.pending_orders: Dict[str, OrderEvent] = {} # {order_id: OrderEvent}
        self.reserved_cash: float = 0.0 # Estimated cash reserved for pending BUY orders + commissions
        self.pending_position_changes: Dict[str, int] = defaultdict(int) # Symbol -> net pending quantity change (+ve BUY, -ve SELL)
        # --------------------------------------------------

        self._last_market_prices: Dict[str, float] = {} # Store latest prices for estimations

        logger.info(f"{self.__class__.__name__} initialized with ${self.initial_cash:.2f} cash.")

    def _setup_event_handlers(self):
        """Subscribe to necessary events."""
        self.event_bus.subscribe(SignalEvent, self._on_signal_event)
        self.event_bus.subscribe(FillEvent, self._on_fill_event)
        self.event_bus.subscribe(MarketEvent, self._on_market_event_for_price)

    async def _on_market_event_for_price(self, event: MarketEvent):
        """Update internal tracker for latest market prices."""
        price = event.data.get('close', event.data.get('price')) # Prefer close, fallback to price
        if price is not None and price > 0:
             self._last_market_prices[event.symbol] = price
        #else:
        #    logger.debug(f"Portfolio: No valid price found in MarketEvent for {event.symbol}")


    async def _on_signal_event(self, event: SignalEvent):
        """Entry point for signal processing. Calls the specific implementation."""
        await self.on_signal(event)

    async def _on_fill_event(self, event: FillEvent):
        """Entry point for fill processing. Updates state then calls specific handler."""
        self._update_state_from_fill(event)
        await self.on_fill(event) # Allow subclasses to react after state is updated

    # --- Helper Methods for Calculation and Checks ---

    def _calculate_commission(self, quantity: int, price: float) -> float:
        """Calculates estimated commission for an order."""
        # Implement your specific commission logic here
        return abs(quantity * price) * DEFAULT_COMMISSION_RATE

    def _estimate_order_cost(self, order: OrderEvent) -> float:
        """
        Estimates the cash impact of placing an order.
        For BUYs, returns positive estimated cost (value + commission).
        For SELLs, returns negative estimated proceeds (value - commission).
        Used primarily for cash reservation check.
        """
        if order.quantity <= 0: return 0.0

        symbol = order.symbol
        quantity = order.quantity
        direction = order.direction

        estimated_price = order.price # Use limit price if available
        if order.order_type == 'MARKET' or estimated_price is None:
            last_price = self.get_latest_price(symbol)
            if last_price is None:
                logger.warning(f"Portfolio Estimate: Cannot estimate cost for MARKET order {order.symbol}, no recent market price.")
                # Fallback or raise error? For now, return 0, which might wrongly pass risk checks.
                # A better fallback might use the position's cost basis if selling?
                return 0.0 # Or some large number to likely fail risk check if buying
            # Apply slippage for market orders
            if direction == "BUY":
                estimated_price = last_price * (1 + DEFAULT_SLIPPAGE_PERCENT)
            elif direction == "SELL":
                estimated_price = last_price * (1 - DEFAULT_SLIPPAGE_PERCENT)
            else:
                 estimated_price = last_price

        if estimated_price <= 0:
            logger.warning(f"Portfolio Estimate: Estimated price for {symbol} is non-positive ({estimated_price:.2f}). Cannot accurately estimate cost.")
            return 0.0

        estimated_value = quantity * estimated_price
        commission = self._calculate_commission(quantity, estimated_price)

        if direction == "BUY":
            return estimated_value + commission
        elif direction == "SELL":
            # Return negative value representing cash inflow before commission deduction
            # However, for risk checks, we usually only care about cash *outflow* (BUYs)
            # Let's return the positive value of proceeds for consistency in magnitude checks if needed elsewhere
            # but for cash reservation, only BUYs matter.
             return estimated_value - commission # Positive value of proceeds
        else:
             return 0.0

    def _check_risk_before_order(self, potential_order: OrderEvent) -> bool:
        """
        Checks if the potential order is permissible given current CONFIRMED state
        and pre-deductions from PENDING orders.
        """
        symbol = potential_order.symbol
        direction = potential_order.direction
        quantity = potential_order.quantity

        # 1. Check Available Cash (most critical for BUYs)
        if direction == "BUY":
            estimated_cost = self._estimate_order_cost(potential_order)
            if estimated_cost <= 0:
                # Handle estimation failure - safer to block order
                logger.warning(f"RISK CHECK: Blocking BUY {symbol} due to inability to estimate cost.")
                return False

            # Check against cash available *after* accounting for already reserved cash
            projected_available_cash = self.cash - self.reserved_cash
            if estimated_cost > projected_available_cash:
                logger.warning(f"RISK CHECK FAILED: Insufficient projected cash for BUY {quantity} {symbol}. "
                               f"Required: ~${estimated_cost:.2f}, Available (Cash - Reserved): ~${projected_available_cash:.2f} "
                               f"(Current Cash: ${self.cash:.2f}, Reserved: ${self.reserved_cash:.2f})")
                return False
            else:
                logger.debug(f"RISK CHECK PASSED (Cash): BUY {quantity} {symbol}. "
                             f"Est. Cost: ~${estimated_cost:.2f}, Projected Available: ~${projected_available_cash:.2f}")

        # 2. Check Position Limits (Example: Prevent excessive shorting if not allowed)
        #    Needs more sophisticated logic for margin accounts.
        #    Simple check: don't allow selling more than projected position if shorting is disallowed.
        allow_shorting = False # Example flag, configure as needed
        if not allow_shorting and direction == "SELL":
            confirmed_qty = self.get_current_position_quantity(symbol)
            pending_change = self.pending_position_changes.get(symbol, 0)
            projected_qty = confirmed_qty + pending_change
            if quantity > projected_qty:
                logger.warning(f"RISK CHECK FAILED: Cannot SELL {quantity} {symbol}. "
                               f"Projected position is {projected_qty} (Confirmed: {confirmed_qty}, Pending: {pending_change}). Shorting not allowed/covered.")
                return False
            else:
                 logger.debug(f"RISK CHECK PASSED (Position): SELL {quantity} {symbol}. Projected Qty: {projected_qty}")

        # Add other checks: max order size, max position concentration, etc.

        return True # Passed all checks

    # --- State Update from Fill ---

    def _update_state_from_fill(self, event: FillEvent):
        """
        Updates portfolio state based on a fill event.
        Handles both confirmed state and reverses pending state deductions.
        """
        order_id = event.order_id
        symbol = event.symbol
        quantity_filled = event.quantity # Quantity actually filled
        price = event.price
        commission = event.commission
        direction = event.direction

        logger.debug(f"Portfolio processing FillEvent: {event}")

        # --- 1. Revert Pending State ---
        if order_id in self.pending_orders:
            original_order = self.pending_orders[order_id] # Get the original order details

            # Recalculate estimated cost based on original order to reverse accurately
            # Note: Use the original order details (quantity might differ from fill if partial)
            estimated_cost_at_placement = self._estimate_order_cost(original_order)

            if original_order.direction == "BUY":
                # Give back the reserved cash
                self.reserved_cash -= estimated_cost_at_placement
                # Should not go below zero, maybe log warning if it does (indicates issue)
                self.reserved_cash = max(0, self.reserved_cash)
                logger.debug(f"Portfolio: Reversed cash reservation of ~${estimated_cost_at_placement:.2f} for BUY order {order_id}. New reserved: ${self.reserved_cash:.2f}")
                # Reduce the pending position change by the original order quantity
                self.pending_position_changes[symbol] -= original_order.quantity

            elif original_order.direction == "SELL":
                # Reduce the pending position change (increase as it's negative)
                self.pending_position_changes[symbol] += original_order.quantity

            logger.debug(f"Portfolio: Updated pending pos change for {symbol} due to fill {order_id}. New pending change: {self.pending_position_changes.get(symbol, 0)}")

            # Remove the order from pending list
            # TODO: Handle partial fills - adjust pending order quantity instead of deleting?
            # For now, assuming full fill or removing on first fill for simplicity.
            del self.pending_orders[order_id]
            logger.debug(f"Portfolio: Removed pending order {order_id}. {len(self.pending_orders)} pending orders remain.")

        else:
            logger.warning(f"Portfolio: Received FillEvent for order_id '{order_id}' which was not found in pending orders. State might be inconsistent.")
            # Continue processing the fill for confirmed state, but be aware of potential issues.

        # --- 2. Update Confirmed State using ACTUAL Fill Data ---
        trade_value = quantity_filled * price
        if direction == "BUY":
            cost = trade_value + commission
            self.cash -= cost
            logger.info(f"Portfolio: CONFIRMED BUY fill for {quantity_filled} of {symbol} at {price}. Cash reduced by {cost:.2f}. New cash: ${self.cash:.2f}")
        elif direction == "SELL":
            proceeds = trade_value - commission
            self.cash += proceeds
            logger.info(f"Portfolio: CONFIRMED SELL fill for {quantity_filled} of {symbol} at {price}. Cash increased by {proceeds:.2f}. New cash: ${self.cash:.2f}")

        # Update position quantity and cost basis
        current_qty = self.positions[symbol]['quantity']
        current_cost_basis = self.positions[symbol]['cost_basis']

        if direction == "BUY":
            new_total_qty = current_qty + quantity_filled
            # Update cost basis only if qty > 0 before fill or if it's the first buy
            if current_qty > 0 :
                 new_total_cost_value = current_qty * current_cost_basis + quantity_filled * price
                 new_cost_basis = new_total_cost_value / new_total_qty if new_total_qty > 0 else 0.0
            else:
                # First buy or buying back into a position after being flat/short
                new_cost_basis = price # Or slightly more complex if covering a short with cost basis tracking
            self.positions[symbol]['quantity'] = new_total_qty
            self.positions[symbol]['cost_basis'] = new_cost_basis

        elif direction == "SELL":
            # Cost basis doesn't change when selling, but quantity reduces
            new_total_qty = current_qty - quantity_filled
            self.positions[symbol]['quantity'] = new_total_qty
            if new_total_qty <= 0:
                 self.positions[symbol]['cost_basis'] = 0.0 # Reset cost basis when flat or short
                 if new_total_qty == 0:
                      # Optional: remove symbol from dict if completely flat
                      # del self.positions[symbol]
                      pass


        logger.info(f"Portfolio: Updated CONFIRMED position for {symbol}. New quantity: {self.positions[symbol]['quantity']}, Avg Cost: ${self.positions[symbol]['cost_basis']:.4f}")


    # --- Accessor Methods ---

    def get_total_portfolio_value(self) -> float:
        """Calculates the total market value of the portfolio (Cash + Holdings)."""
        total_value = self.cash # Start with confirmed cash
        for symbol, pos_info in self.positions.items():
            quantity = pos_info['quantity']
            if quantity != 0:
                 latest_price = self._last_market_prices.get(symbol)
                 if latest_price is not None and latest_price > 0:
                      total_value += quantity * latest_price
                 else:
                      logger.warning(f"Portfolio Value: Cannot get valid latest price for {symbol} ({latest_price}) to calculate total value. Using cost basis estimate: {quantity * pos_info['cost_basis']:.2f}")
                      total_value += quantity * pos_info['cost_basis'] # Fallback

        # Optional: Adjust for reserved cash? Subtracting reserved_cash might give a
        # more conservative "available equity" view, but total value usually includes all cash.
        # total_value -= self.reserved_cash # Uncomment for conservative view

        return total_value

    def get_current_position_quantity(self, symbol: str) -> int:
        """Gets the confirmed position quantity for a symbol."""
        return self.positions.get(symbol, {}).get('quantity', 0)

    def get_projected_position_quantity(self, symbol: str) -> int:
        """Gets the projected position quantity (Confirmed + Pending)."""
        confirmed_qty = self.get_current_position_quantity(symbol)
        pending_change = self.pending_position_changes.get(symbol, 0)
        return confirmed_qty + pending_change

    def get_available_cash(self) -> float:
        """Gets the cash available for new trades (Confirmed Cash - Reserved Cash)."""
        return self.cash - self.reserved_cash

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Gets the last known market price for a symbol."""
        price = self._last_market_prices.get(symbol)
        if price is not None and price > 0:
            return price
        logger.debug(f"Portfolio Price: No valid latest price available for {symbol}.")
        return None

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
    and adheres to lot size constraints, using pending order tracking.
    """
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0, lot_size: int = 100):
        super().__init__(event_bus, initial_cash)
        self.lot_size = max(1, lot_size) # Ensure lot_size is at least 1
        logger.info(f"{self.__class__.__name__} initialized with lot_size={self.lot_size}.")

    async def on_signal(self, signal_event: SignalEvent):
        logger.info(f"收到信号: {signal_event}")
        symbol = signal_event.symbol
        direction = signal_event.direction # Expected: LONG, SHORT, FLAT
        weight = signal_event.weight     # Expected: Target weight (e.g., 0.1 for 10%) or None for FLAT

        # --- 1. Handle FLAT Signal Immediately ---
        if direction == "FLAT":
            await self._generate_flat_order(symbol)
            return

        # --- 2. Validate LONG/SHORT Signal ---
        if direction not in ["LONG", "SHORT"] or weight is None or weight < 0:
            logger.warning(f"MomentumPortfolio: Received invalid or unsupported signal: {signal_event}. No order generated.")
            return

        # --- 3. Get Necessary Data ---
        latest_price = self.get_latest_price(symbol)
        if latest_price is None:
            logger.warning(f"MomentumPortfolio: Cannot get latest price for {symbol}. Cannot process signal {signal_event.id}.")
            return

        total_portfolio_value = self.get_total_portfolio_value()
        if total_portfolio_value <= 0:
            logger.warning(f"MomentumPortfolio: Total portfolio value is zero or negative (${total_portfolio_value:.2f}). Cannot calculate target.")
            return

        # --- 4. Calculate Target vs Projected ---
        # Target value based on total portfolio value and desired weight
        target_value = total_portfolio_value * weight
        target_value = max(0, target_value) # Ensure non-negative

        # Calculate target quantity (float) based on price
        target_quantity_float = target_value / latest_price

        # Get PROJECTED quantity (Confirmed + Pending)
        projected_quantity = self.get_projected_position_quantity(symbol)

        # Calculate the difference needed vs PROJECTED state
        quantity_difference_float = target_quantity_float - projected_quantity
        logger.debug(f"MomentumPortfolio: {symbol} | Target Weight: {weight:.2%} | Total Value: ${total_portfolio_value:.2f} | Target Value: ${target_value:.2f} | Target Qty: {target_quantity_float:.2f} | Proj Qty: {projected_quantity} | Qty Diff vs Proj: {quantity_difference_float:.2f}")

        # --- 5. Determine Order Quantity and Direction (Apply Lot Size) ---
        order_quantity_int = 0
        order_direction: Optional[str] = None

        # Only act if the needed quantity change is significant (e.g., >= lot size)
        # Adjust threshold logic as needed (e.g., half lot size?)
        action_threshold = self.lot_size * 0.95 # Be slightly lenient to trigger rounding

        if quantity_difference_float > action_threshold: # Need to increase position (Buy)
             order_direction = "BUY"
             # Round DOWN to nearest multiple of lot_size
             order_quantity_int = int(math.floor(quantity_difference_float / self.lot_size) * self.lot_size)

        elif quantity_difference_float < -action_threshold: # Need to decrease position (Sell)
             order_direction = "SELL"
             # Round DOWN the absolute difference to nearest multiple of lot_size
             order_quantity_int = int(math.floor(abs(quantity_difference_float) / self.lot_size) * self.lot_size)

        else:
             logger.debug(f"MomentumPortfolio: {symbol} | Difference vs projected ({quantity_difference_float:.2f}) is less than threshold ({action_threshold:.2f}). No order needed.")
             return

        # --- 6. Final Checks and Order Placement ---
        if order_quantity_int <= 0 or order_direction is None:
            logger.debug(f"MomentumPortfolio: Final calculated order quantity is zero or negative ({order_quantity_int}). No order generated.")
            return

        # Create a potential order object for risk checking
        potential_order = OrderEvent(
            symbol=symbol,
            direction=order_direction,
            quantity=order_quantity_int,
            order_type="MARKET" # Assuming MARKET orders from momentum signals
            # id and timestamp generated if published
        )

        # --- Perform Risk Check BEFORE deciding to publish ---
        if self._check_risk_before_order(potential_order):
            # Risk check passed, create the final OrderEvent
            order_event = OrderEvent(
                symbol=potential_order.symbol,
                direction=potential_order.direction,
                quantity=potential_order.quantity,
                order_type=potential_order.order_type
                # Add timestamp and unique ID here upon creation if needed by OrderEvent dataclass
            )

            # --- PRE-DEDUCTION and PENDING STATE UPDATE ---
            self.pending_orders[order_event.id] = order_event
            estimated_impact = self._estimate_order_cost(order_event)

            if order_event.direction == "BUY":
                self.reserved_cash += estimated_impact
                self.pending_position_changes[symbol] += order_event.quantity
                logger.debug(f"MomentumPortfolio: Reserved ~${estimated_impact:.2f} cash for pending BUY {order_event.id}. Total reserved: ${self.reserved_cash:.2f}")
            elif order_event.direction == "SELL":
                # Typically don't reserve cash unless shorting margin needed
                self.pending_position_changes[symbol] -= order_event.quantity
                logger.debug(f"MomentumPortfolio: No cash reserved for pending SELL {order_event.id}.")

            logger.debug(f"MomentumPortfolio: Updated pending pos change for {symbol} due to new order {order_event.id}. New pending change: {self.pending_position_changes.get(symbol, 0)}")
            logger.info(f"MomentumPortfolio: Publishing OrderEvent: {order_event}")
            # --- Publish the order ---
            self.event_bus.publish(order_event) # Use await if publish is async

        else:
            # Risk check failed, logged within _check_risk_before_order
            logger.warning(f"MomentumPortfolio: Order generation for {potential_order} blocked by risk check.")


    async def _generate_flat_order(self, symbol: str):
        """Generates an order to close the current CONFIRMED position for a symbol."""
        current_quantity = self.get_current_position_quantity(symbol) # Check confirmed quantity

        if current_quantity == 0:
            logger.debug(f"MomentumPortfolio: Received FLAT signal for {symbol}, but confirmed position is already zero.")
            # Optional: Check if there are pending orders that need cancelling?
            return

        # Determine direction needed to close the confirmed position
        order_direction = "SELL" if current_quantity > 0 else "BUY"
        order_quantity_int = abs(current_quantity)

        logger.info(f"MomentumPortfolio: Received FLAT signal for {symbol}. Generating order to close confirmed position ({order_direction} {order_quantity_int}).")

        # Create potential closing order
        potential_order = OrderEvent(
             symbol=symbol,
             direction=order_direction,
             quantity=order_quantity_int,
             order_type="MARKET"
        )

        # Perform risk check (e.g., ensure sell is covered, basic cash check if buying to cover)
        if self._check_risk_before_order(potential_order):
            # Create final event
             order_event = OrderEvent(
                  symbol=potential_order.symbol,
                  direction=potential_order.direction,
                  quantity=potential_order.quantity,
                  order_type=potential_order.order_type
             )

             # --- Add to Pending State ---
             self.pending_orders[order_event.id] = order_event
             estimated_impact = self._estimate_order_cost(order_event) # Estimate impact

             if order_event.direction == "BUY": # Buying to cover short
                  self.reserved_cash += estimated_impact
                  self.pending_position_changes[symbol] += order_event.quantity # Should bring pending change closer to zero
                  logger.debug(f"MomentumPortfolio: Reserved ~${estimated_impact:.2f} cash for pending BUY-to-cover {order_event.id}. Total reserved: ${self.reserved_cash:.2f}")
             elif order_event.direction == "SELL": # Selling to close long
                  self.pending_position_changes[symbol] -= order_event.quantity # Should bring pending change closer to zero
                  logger.debug(f"MomentumPortfolio: No cash reserved for pending SELL-to-close {order_event.id}.")

             logger.debug(f"MomentumPortfolio: Updated pending pos change for {symbol} due to FLAT order {order_event.id}. New pending change: {self.pending_position_changes.get(symbol, 0)}")
             logger.info(f"MomentumPortfolio: Publishing FLAT OrderEvent: {order_event}")
             self.event_bus.publish(order_event) # Use await if publish is async
        else:
             logger.warning(f"MomentumPortfolio: FLAT Order generation for {potential_order} blocked by risk check.")


    async def on_fill(self, fill_event: FillEvent):
        """Log portfolio summary after a fill updates the state."""
        # BasePortfolio._update_state_from_fill has already run and updated state
        logger.info(f"MomentumPortfolio Post-Fill Summary: Cash: ${self.cash:.2f}, Reserved Cash: ${self.reserved_cash:.2f}, Available Cash: ${self.get_available_cash():.2f}, Total Value: ${self.get_total_portfolio_value():.2f}")
