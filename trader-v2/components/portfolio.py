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
from core.events import SignalEvent, OrderEvent, FillEvent, MarketEvent

logger = logging.getLogger(__name__)

class BasePortfolio(BaseComponent):
    # ... (BasePortfolio class code remains the same as previous revision) ...
    """
    Base class for Portfolio management.
    Handles position keeping, cash management, and generates orders from signals.
    Includes common methods for state tracking and valuation.
    """
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0):
        super().__init__(event_bus)
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'quantity': 0, 'cost_basis': 0.0})
        self.pending_orders: Dict[str, OrderEvent] = {}
        self._last_market_prices: Dict[str, float] = {}

        logger.info(f"{self.__class__.__name__} initialized with ${self.initial_cash:.2f} cash.")


    def _setup_event_handlers(self):
        self.event_bus.subscribe(SignalEvent, self._on_signal_event)
        self.event_bus.subscribe(FillEvent, self._on_fill_event)
        self.event_bus.subscribe(MarketEvent, self._on_market_event_for_price)


    async def _on_market_event_for_price(self, event: MarketEvent):
        if 'close' in event.data:
             self._last_market_prices[event.symbol] = event.data['close']
        elif 'price' in event.data:
             self._last_market_prices[event.symbol] = event.data['price']


    async def _on_signal_event(self, event: SignalEvent):
        logger.info('------poprtfolio event handlers------')
        await self.on_signal(event)


    async def _on_fill_event(self, event: FillEvent):
        self._update_state_from_fill(event)
        await self.on_fill(event)

    def _update_state_from_fill(self, event: FillEvent):
        symbol = event.symbol
        quantity = event.quantity
        price = event.price
        commission = event.commission
        direction = event.direction

        trade_value = quantity * price
        if direction == "BUY":
            cost = trade_value + commission
            self.cash -= cost
            logger.info(f"Portfolio: BUY fill for {quantity} of {symbol} at {price}. Cash reduced by {cost:.2f}. New cash: ${self.cash:.2f}")
        elif direction == "SELL":
            proceeds = trade_value - commission
            self.cash += proceeds
            logger.info(f"Portfolio: SELL fill for {quantity} of {symbol} at {price}. Cash increased by {proceeds:.2f}. New cash: ${self.cash:.2f}")

        current_qty = self.positions[symbol]['quantity']
        current_cost_basis = self.positions[symbol]['cost_basis']

        if direction == "BUY":
            new_total_qty = current_qty + quantity
            new_total_cost_value = current_qty * current_cost_basis + quantity * price
            self.positions[symbol]['quantity'] = new_total_qty
            if new_total_qty > 0:
                 self.positions[symbol]['cost_basis'] = new_total_cost_value / new_total_qty
            else:
                 self.positions[symbol]['cost_basis'] = 0.0
        elif direction == "SELL":
            new_total_qty = current_qty - quantity
            self.positions[symbol]['quantity'] = new_total_qty
            if new_total_qty <= 0:
                 self.positions[symbol]['cost_basis'] = 0.0


        logger.info(f"Portfolio: Updated position for {symbol}. New quantity: {self.positions[symbol]['quantity']}, Avg Cost: ${self.positions[symbol]['cost_basis']:.2f}")

        if event.order_id in self.pending_orders:
             # In a real system, handle partial fills by reducing pending order quantity.
             # For simplicity here, we assume full fills or remove on first fill.
             # A more robust approach would need to track filled quantity vs ordered quantity per order ID.
             del self.pending_orders[event.order_id]
             logger.debug(f"Portfolio: Removed pending order {event.order_id}")


    def get_total_portfolio_value(self) -> float:
        total_value = self.cash
        for symbol, pos_info in self.positions.items():
            quantity = pos_info['quantity']
            if quantity != 0:
                 latest_price = self._last_market_prices.get(symbol)
                 if latest_price is not None and latest_price > 0:
                      total_value += quantity * latest_price
                 else:
                      logger.warning(f"Portfolio: Cannot get valid latest price for {symbol} ({latest_price}) to calculate total value. Using cost basis estimate for position value.")
                      total_value += quantity * pos_info['cost_basis'] # Fallback to cost basis

        return total_value

    def get_current_position_quantity(self, symbol: str) -> int:
        return self.positions.get(symbol, {}).get('quantity', 0)

    def get_latest_price(self, symbol: str) -> Optional[float]:
        price = self._last_market_prices.get(symbol)
        if price is not None and price > 0:
            return price
        return None

    async def on_signal(self, signal_event: SignalEvent):
        pass

    async def on_fill(self, fill_event: FillEvent):
         pass


# --- Momentum Portfolio Implementation ---

class MomentumPortfolio(BasePortfolio):
    """
    A portfolio implementation for the momentum strategy.
    Calculates order quantity based on signal weight (percentage of total portfolio value)
    and adheres to lot size constraints.
    """
    # --- Rename min_trade_unit to lot_size and set default to 100 ---
    def __init__(self, event_bus: EventBus, initial_cash: float = 100000.0, lot_size: int = 100):
        super().__init__(event_bus, initial_cash)
        self.lot_size = lot_size # Store lot size
        logger.info(f"{self.__class__.__name__} initialized with lot_size={self.lot_size}.")
    # -------------------------------------------------------------


    async def on_signal(self, signal_event: SignalEvent):
        logger.debug(f"MomentumPortfolio: Received signal event: {signal_event}")
        symbol = signal_event.symbol
        direction = signal_event.direction
        weight = signal_event.weight

        # --- 1. Validate Signal ---
        if weight is None or weight < 0:
             if direction == "FLAT":
                  await self._generate_flat_order(symbol) # Handle FLAT signal
             else:
                  logger.debug(f"MomentumPortfolio: Received signal with invalid weight ({weight}) for {symbol}. No order generated.")
             return

        # --- 2. Get Necessary Data ---
        latest_price = self.get_latest_price(symbol)
        if latest_price is None:
            logger.warning(f"MomentumPortfolio: Cannot get latest price for {symbol}. Cannot process signal {signal_event.id}.")
            return

        total_portfolio_value = self.get_total_portfolio_value()
        current_quantity = self.get_current_position_quantity(symbol)


        # --- 3. Calculate Target and Difference ---
        # Target value based on total portfolio value and desired weight
        target_value = total_portfolio_value * weight
        # Ensure target_value is non-negative
        target_value = max(0, target_value)

        # Calculate target quantity (float)
        if latest_price <= 0:
             logger.warning(f"MomentumPortfolio: Latest price for {symbol} is zero or negative ({latest_price}). Cannot calculate target quantity.")
             return
        target_quantity_float = target_value / latest_price

        # Calculate the difference needed to reach the target quantity
        quantity_difference_float = target_quantity_float - current_quantity
        logger.debug(f"MomentumPortfolio: {symbol} - Total value: ${total_portfolio_value:.2f}, Target value: ${target_value:.2f}, Current Qty: {current_quantity}, Target Qty (float): {target_quantity_float:.2f}, Qty Difference (float): {quantity_difference_float:.2f}")


        # --- 4. Determine Order Quantity and Direction (Apply Lot Size Constraint) ---
        order_quantity_int = 0
        order_direction: Optional[str] = None

        # Define a minimum threshold for action based on lot size
        # Only act if the needed quantity is at least one lot size
        action_threshold = self.lot_size

        if direction == "LONG":
             # Need to buy if current quantity is less than target (positive difference)
             if quantity_difference_float > action_threshold:
                 order_direction = "BUY"
                 # Calculate quantity to buy, rounding DOWN to the nearest multiple of lot_size
                 order_quantity_float = math.floor(quantity_difference_float / self.lot_size) * self.lot_size
                 order_quantity_int = int(order_quantity_float)
                 logger.debug(f"MomentumPortfolio: LONG signal, need to buy. Calculated quantity (raw): {quantity_difference_float:.2f}, rounded down to nearest {self.lot_size}: {order_quantity_int}")

             elif quantity_difference_float < -action_threshold:
                 # Long signal, but current position is too large or short. Need to sell.
                 # Calculate quantity to sell, rounding DOWN the absolute value to nearest multiple of lot_size
                 order_direction = "SELL"
                 order_quantity_float = math.floor(abs(quantity_difference_float) / self.lot_size) * self.lot_size
                 order_quantity_int = int(order_quantity_float)
                 logger.debug(f"MomentumPortfolio: LONG signal, but need to reduce position/cover short. Calculated quantity to sell: {order_quantity_int}")

             else:
                 logger.debug(f"MomentumPortfolio: LONG signal for {symbol}. Difference {quantity_difference_float:.2f} less than {self.lot_size}. No order generated.")


        elif direction == "SHORT":
             # Need to sell if current quantity is more than target (negative difference)
             if quantity_difference_float < -action_threshold:
                 order_direction = "SELL"
                 order_quantity_float = math.floor(abs(quantity_difference_float) / self.lot_size) * self.lot_size
                 order_quantity_int = int(order_quantity_float)
                 logger.debug(f"MomentumPortfolio: SHORT signal, need to sell. Calculated quantity (raw): {quantity_difference_float:.2f}, rounded down to nearest {self.lot_size}: {order_quantity_int}")

             elif quantity_difference_float > action_threshold:
                 # Short signal, but current position is too small or long. Need to buy (cover).
                 order_direction = "BUY"
                 order_quantity_float = math.floor(quantity_difference_float / self.lot_size) * self.lot_size
                 order_quantity_int = int(order_quantity_float)
                 logger.debug(f"MomentumPortfolio: SHORT signal, but need to buy (cover). Calculated quantity to buy: {order_quantity_int}")
             else:
                 logger.debug(f"MomentumPortfolio: SHORT signal for {symbol}. Difference {quantity_difference_float:.2f} less than {self.lot_size}. No order generated.")


        elif direction == "FLAT":
             # Signal to close position - close exact quantity held, lot size doesn't apply here for closing
             await self._generate_flat_order(symbol)
             return # Order generated by _generate_flat_order


        else:
             logger.warning(f"MomentumPortfolio: Received unknown signal direction: {direction} for {symbol}. No order generated.")
             return


        # --- 5. Final Checks and Order Placement ---
        # Ensure calculated quantity is valid and positive after rounding
        if order_quantity_int <= 0 or order_direction is None:
            logger.debug(f"MomentumPortfolio: Final calculated order quantity is zero ({order_quantity_int}) or direction is None. No order generated.")
            return

        # Basic Cash Check for BUY orders
        if order_direction == "BUY":
            estimated_cost = order_quantity_int * latest_price # Minimal cost estimate
            # Add a buffer for slippage/commission in a real system
            # estimated_cost = order_quantity_int * latest_price * (1 + buffer_percentage + commission_percentage)
            if estimated_cost > self.cash:
                 logger.warning(f"MomentumPortfolio: Insufficient cash (${self.cash:.2f}) for BUY order of {order_quantity_int} {symbol}. Estimated cost: ${estimated_cost:.2f}. No order generated.")
                 return


        # If selling, ensure we hold at least the quantity being sold (in this simple model)
        # _generate_flat_order handles the exact quantity check for closing.
        # If selling to reduce position, ensure we don't go excessively short if not allowed.
        if order_direction == "SELL" and self.get_current_position_quantity(symbol) < order_quantity_int:
             logger.warning(f"MomentumPortfolio: Attempted to SELL {order_quantity_int} of {symbol} but only {self.get_current_position_quantity(symbol)} held. Shorting not fully implemented. No order generated.")
             return


        # Create and publish OrderEvent
        order_event = OrderEvent(
            symbol=symbol,
            direction=order_direction,
            quantity=order_quantity_int,
            order_type="MARKET"
        )

        # Store pending order
        self.pending_orders[order_event.id] = order_event
        logger.info(f"MomentumPortfolio: Publishing OrderEvent: {order_event}")
        self.event_bus.publish(order_event)


    async def _generate_flat_order(self, symbol: str):
        """Generates an order to close the current position for a symbol."""
        current_quantity = self.get_current_position_quantity(symbol)

        if current_quantity == 0:
            logger.debug(f"MomentumPortfolio: Received FLAT signal for {symbol}, but position is already zero.")
            return

        order_direction = "SELL" if current_quantity > 0 else "BUY" # Buy to cover short, sell to liquidate long
        order_quantity_int = abs(current_quantity) # Close the entire position

        logger.info(f"MomentumPortfolio: Received FLAT signal for {symbol}. Generating order to close position ({order_direction} {order_quantity_int}).")

        # Check if we hold the quantity needed to sell (if closing a long)
        if order_direction == "SELL" and self.get_current_position_quantity(symbol) < order_quantity_int:
             logger.warning(f"MomentumPortfolio: Cannot close position for {symbol}. Attempted to sell {order_quantity_int} but only {self.get_current_position_quantity(symbol)} held. No order generated.")
             return


        order_event = OrderEvent(
             symbol=symbol,
             direction=order_direction,
             quantity=order_quantity_int,
             order_type="MARKET"
        )
        self.pending_orders[order_event.id] = order_event
        logger.info(f"MomentumPortfolio: Publishing FLAT OrderEvent: {order_event}")
        self.event_bus.publish(order_event)


    async def on_fill(self, fill_event: FillEvent):
         """Handle fill event in momentum portfolio (after state update)."""
         # BasePortfolio._update_state_from_fill has already run
         logger.info(f"MomentumPortfolio: Fill processed for order {fill_event.order_id} (Symbol: {fill_event.symbol}). "
                     f"Current Cash: ${self.cash:.2f}, "
                     f"Current Position: {self.get_current_position_quantity(fill_event.symbol)} {fill_event.symbol}")