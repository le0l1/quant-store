import logging
from datetime import datetime
from typing import Any, Dict, Optional
import uuid
import math
import pandas as pd

from collections import defaultdict

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import SignalEvent, OrderEvent, MarketEvent
from components.data_feed import BaseDataFeed

logger = logging.getLogger(__name__)

# --- Configuration Constants ---
DEFAULT_COMMISSION_RATE = 0.0005  # 0.05%
DEFAULT_SLIPPAGE_PERCENT = 0.001  # 0.1% for market order estimations

# --- Base Trading System Class ---

class BaseBroker(BaseComponent):
    """
    Base class for a trading system combining portfolio management and order execution.
    Manages cash, positions, and processes signals and market events.
    """
    def __init__(self, event_bus: EventBus, data_feed: BaseDataFeed, initial_cash: float = 100000.0):
        super().__init__(event_bus)
        self.data_feed = data_feed
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'quantity': 0, 'cost_basis': 0.0})
        self.nav_history: Dict[datetime, float] = {}
        self._last_nav_timestamp: Optional[datetime] = None
        logger.info(f"{self.__class__.__name__} initialized with ${self.initial_cash:.2f} cash.")

    def _setup_event_handlers(self):
        self.event_bus.subscribe(SignalEvent, self._on_signal_event)
        self.event_bus.subscribe(MarketEvent, self._on_market_event)

    async def _on_signal_event(self, event: SignalEvent):
        await self.on_signal(event)

    async def _on_market_event(self, event: MarketEvent):
        await self.on_market_event(event)
        if hasattr(event, 'timestamp') and isinstance(event.timestamp, datetime):
            self._record_nav(event.timestamp)
    def _calculate_commission(self, quantity: int, price: float) -> float:
        return abs(quantity * price) * DEFAULT_COMMISSION_RATE

    def _estimate_order_cost(self, order: OrderEvent) -> float:
        if order.quantity <= 0:
            return 0.0
        symbol = order.symbol
        quantity = order.quantity
        direction = order.direction
        estimated_price = order.price
        if order.order_type == 'MARKET' or estimated_price is None:
            last_price = self.get_latest_price(symbol)
            if last_price is None:
                logger.warning(f"Estimate: Cannot estimate cost for MARKET order {symbol}, no recent price.")
                return 0.0
            estimated_price = last_price * (1 + DEFAULT_SLIPPAGE_PERCENT) if direction == "BUY" else last_price
        if estimated_price <= 0:
            logger.warning(f"Estimate: Estimated price for {symbol} is non-positive ({estimated_price:.2f}).")
            return 0.0
        estimated_value = quantity * estimated_price
        commission = self._calculate_commission(quantity, estimated_price)
        return estimated_value + commission if direction == "BUY" else estimated_value - commission

    def _check_risk_before_order(self, order: OrderEvent) -> bool:
        symbol = order.symbol
        direction = order.direction
        quantity = order.quantity
        if quantity <= 0:
            logger.warning(f"RISK CHECK: Blocking order for {symbol} with non-positive quantity ({quantity}).")
            return False
        if direction == "BUY":
            estimated_cost = self._estimate_order_cost(order)
            if estimated_cost <= 0:
                logger.warning(f"RISK CHECK: Blocking BUY order for {symbol} due to inability to estimate cost.")
                return False
            if estimated_cost > self.cash:
                logger.warning(f"RISK CHECK FAILED: Insufficient cash for BUY {quantity} {symbol}. "
                              f"Required: ~${estimated_cost:.2f}, Available: ${self.cash:.2f}")
                return False
        allow_shorting = False
        if not allow_shorting and direction == "SELL":
            confirmed_qty = self.get_current_position_quantity(symbol)
            if quantity > confirmed_qty:
                logger.warning(f"RISK CHECK FAILED: Cannot SELL {quantity} {symbol}. Available: {confirmed_qty}")
                return False
        logger.debug(f"RISK CHECK PASSED for order: {order}")
        return True

    def get_total_portfolio_value(self) -> float:
        total_value = self.cash
        for symbol, pos_info in self.positions.items():
            quantity = pos_info['quantity']
            if quantity != 0:
                latest_price = self.get_latest_price(symbol)
                if latest_price is not None and latest_price > 0:
                    total_value += quantity * latest_price
                else:
                    logger.warning(f"Portfolio Value: No valid price for {symbol}. Using cost basis.")
                    total_value += quantity * pos_info['cost_basis']
        return total_value

    def get_current_position_quantity(self, symbol: str) -> int:
        return self.positions.get(symbol, {}).get('quantity', 0)

    def get_positions(self):
        data = []
        for symbol, pos_info in self.positions.items():
            current_price = self.get_latest_price(symbol)
            quantity = pos_info['quantity']
            cost_basis = pos_info['cost_basis']
            if current_price is not None and current_price > 0:
                profit_loss = (current_price - cost_basis) * quantity
                profit_loss_pct = (profit_loss / (cost_basis * quantity)) if cost_basis != 0 else 0
            else:
                profit_loss = 0
                profit_loss_pct = 0
            data.append({
                'symbol': symbol,
                'quantity': quantity,
                'cost_basis': cost_basis,
                'current_price': current_price if current_price is not None else 0,
                'profit_loss': profit_loss,
                'profit_loss_pct': profit_loss_pct
            })
        return pd.DataFrame(data)

    def get_available_cash(self) -> float:
        return self.cash

    def get_latest_price(self, symbol: str) -> Optional[float]:
        try:
            return self.data_feed.get_latest_price(symbol)
        except Exception as e:
            logger.error(f"Error getting latest price for {symbol}: {str(e)}")
            return None

    def _record_nav(self, timestamp: datetime):
        if self._last_nav_timestamp and timestamp <= self._last_nav_timestamp:
            return
        total_value = self.get_total_portfolio_value()
        self.nav_history[timestamp] = total_value
        self._last_nav_timestamp = timestamp

    async def on_signal(self, signal_event: SignalEvent):
        raise NotImplementedError("Subclasses must implement on_signal")

    async def on_market_event(self, market_event: MarketEvent):
        raise NotImplementedError("Subclasses must implement on_market_event")

# --- Concrete Trading System Class ---

class MomentumBroker(BaseBroker):
    """
    A trading system implementing a momentum strategy with simulated order execution.
    Generates orders based on signal weights and updates state directly on market events.
    """
    def __init__(self, event_bus: EventBus, data_feed: BaseDataFeed, initial_cash: float = 100000.0, lot_size: int = 1,
                 commission_percent: float = DEFAULT_COMMISSION_RATE, slippage_percent: float = DEFAULT_SLIPPAGE_PERCENT):
        super().__init__(event_bus, data_feed, initial_cash)
        self.lot_size = max(1, int(lot_size))
        self.commission_percent = commission_percent
        self.slippage_percent = slippage_percent
        self._pending_orders: Dict[str, OrderEvent] = {}
        self._last_market_timestamp: Optional[datetime] = None
        logger.info(f"{self.__class__.__name__} initialized with lot_size={self.lot_size}, "
                    f"commission={self.commission_percent:.4f}, slippage={self.slippage_percent:.4f}.")

    async def on_signal(self, signal_event: SignalEvent):
        symbol = signal_event.symbol
        direction = signal_event.direction
        weight = signal_event.weight
        signal_time = signal_event.timestamp
        if direction == "FLAT":
            confirmed_qty = self.get_current_position_quantity(symbol)
            if confirmed_qty == 0:
                logger.debug(f"Already flat for {symbol}.")
                return
            quantity_to_flatten = abs(confirmed_qty)
            if quantity_to_flatten > 0:
                await self._generate_flat_order(symbol, quantity_to_flatten, signal_time)
            return
        if direction not in ["LONG", "SHORT"] or weight is None or weight < 0:
            logger.warning(f"Invalid signal direction '{direction}' or weight '{weight}' for {symbol}.")
            return
        latest_price = self.get_latest_price(symbol)
        if latest_price is None or latest_price <= 0:
            logger.warning(f"No valid price for {symbol} ({latest_price}).")
            return
        total_portfolio_value = self.get_total_portfolio_value()
        if total_portfolio_value <= 1e-6:
            logger.warning(f"Portfolio value near zero (${total_portfolio_value:.2f}).")
            return
        target_value = total_portfolio_value * weight
        target_value = max(0.0, target_value)
        target_quantity_float = target_value / latest_price if latest_price > 1e-9 else 0.0
        current_quantity = self.get_current_position_quantity(symbol)
        adjusted_target_quantity_float = target_quantity_float if direction == "LONG" else -target_quantity_float
        quantity_difference_float = adjusted_target_quantity_float - current_quantity
        logger.debug(f"Calc: {symbol} | Sig Dir: {direction}, Weight: {weight:.2%} | "
                     f"Target Qty: {adjusted_target_quantity_float:.2f} | Current Qty: {current_quantity} | "
                     f"Qty Diff: {quantity_difference_float:.2f}")
        order_quantity_int = 0
        order_direction = None
        if quantity_difference_float > self.lot_size * 0.99:
            order_direction = "BUY"
            order_quantity_int = int(math.floor(quantity_difference_float / self.lot_size) * self.lot_size)
        elif quantity_difference_float < -self.lot_size * 0.99:
            order_direction = "SELL"
            order_quantity_int = int(math.floor(abs(quantity_difference_float) / self.lot_size) * self.lot_size)
        else:
            logger.debug(f"Qty difference ({quantity_difference_float:.2f}) < lot size ({self.lot_size}).")
            return
        if order_quantity_int <= 0 or order_direction is None:
            logger.debug(f"Zero or negative quantity ({order_quantity_int}).")
            return
        order_event = OrderEvent(
            id=str(uuid.uuid4()),
            timestamp=signal_time,
            symbol=symbol,
            direction=order_direction,
            quantity=order_quantity_int,
            order_type="MARKET",
            price=None
        )
        if self._check_risk_before_order(order_event):
            logger.info(f"Queuing OrderEvent: {order_event}")
            self._pending_orders[order_event.id] = order_event
        else:
            logger.warning(f"Order for {order_event} blocked by risk check.")

    async def _generate_flat_order(self, symbol: str, quantity: int, signal_time: datetime):
        current_quantity = self.get_current_position_quantity(symbol)
        if current_quantity == 0 or quantity <= 0:
            logger.debug(f"No FLAT order for {symbol} (qty: {current_quantity}, requested: {quantity}).")
            return
        order_direction = "SELL" if current_quantity > 0 else "BUY"
        order_event = OrderEvent(
            id=str(uuid.uuid4()),
            timestamp=signal_time,
            symbol=symbol,
            direction=order_direction,
            quantity=quantity,
            order_type="MARKET",
            price=None
        )
        if self._check_risk_before_order(order_event):
            logger.info(f"Queuing FLAT OrderEvent: {order_event}")
            self._pending_orders[order_event.id] = order_event
        else:
            logger.warning(f"FLAT Order for {order_event} blocked by risk check.")
    async def on_market_event(self, market_event: MarketEvent):
        current_timestamp = market_event.timestamp
        is_new_time_step = (self._last_market_timestamp is None or current_timestamp > self._last_market_timestamp)
        if not is_new_time_step:
            logger.debug(f"Still processing timestamp {current_timestamp}.")
            return

        orders_to_settle_ids = list(self._pending_orders.keys())
        for order_id in orders_to_settle_ids:
            order = self._pending_orders.get(order_id)
            if not order:
                logger.warning(f"Pending order {order_id} not found during settlement.")
                continue

            symbol = order.symbol
            order_quantity = order.quantity # This is the absolute quantity of the order
            direction = order.direction

            latest_price = self.data_feed.get_latest_price(symbol)
            if latest_price is None or latest_price <= 0:
                logger.warning(f"Invalid or missing price for {symbol} ({latest_price}) at {current_timestamp}. Skipping order {order_id}.")
                continue
            
            base_simulated_price = latest_price
            final_fill_price = base_simulated_price
            slippage_amount = base_simulated_price * self.slippage_percent

            if direction == "BUY":
                final_fill_price += slippage_amount # Buyer gets slightly worse price
            elif direction == "SELL":
                final_fill_price -= slippage_amount # Seller gets slightly worse price

            if final_fill_price <= 0:
                logger.warning(f"Final fill price for {symbol} is non-positive ({final_fill_price}) after slippage. Skipping order {order_id}.")
                continue

            simulated_commission = final_fill_price * order_quantity * self.commission_percent

            # Initialize position if it doesn't exist
            if symbol not in self.positions:
                self.positions[symbol] = {'quantity': 0.0, 'cost_basis': 0.0}

            current_qty = self.positions[symbol]['quantity']
            current_cost_basis = self.positions[symbol]['cost_basis']

            if direction == "BUY":
                cost = (order_quantity * final_fill_price) + simulated_commission
                if cost > self.cash and (current_qty + order_quantity > 0): # Only check if not covering a short that would net cash or be cash neutral
                    # More sophisticated check needed if allowing leveraged short covering
                    logger.warning(f"Insufficient cash for BUY order {order_id} for {symbol}. Cost: {cost:.2f}, Cash: {self.cash:.2f}. Skipping.")
                    continue
                
                self.cash -= cost
                
                new_total_qty = current_qty + order_quantity
                
                if current_qty < 0: # Was short
                    qty_covered = min(order_quantity, abs(current_qty))
                    qty_going_long = order_quantity - qty_covered
                    
                    # P&L for covered short part is implicitly realized in cash change
                    # For cost basis, if we flip to long, new basis is for the long part
                    if new_total_qty > 0: # Flipped to long
                        self.positions[symbol]['cost_basis'] = final_fill_price # Cost basis for the new long shares
                    elif new_total_qty == 0: # Flattened
                        self.positions[symbol]['cost_basis'] = 0.0
                    else: # Reduced short position
                        # Cost basis (avg short price) remains the same for remaining short
                        pass
                else: # Was flat or long
                    if new_total_qty > 0: # Avoid division by zero if current_qty and order_quantity are 0 (though order_quantity > 0)
                        new_cost_basis = (current_qty * current_cost_basis + order_quantity * final_fill_price) / new_total_qty
                        self.positions[symbol]['cost_basis'] = new_cost_basis
                    else: # Should not happen if current_qty >=0 and order_qty > 0
                        logger.error(f"Unexpected new_total_qty {new_total_qty} in BUY logic for {symbol}")
                        self.positions[symbol]['cost_basis'] = 0.0


                self.positions[symbol]['quantity'] = new_total_qty
                if new_total_qty == 0:
                     self.positions[symbol]['cost_basis'] = 0.0


            elif direction == "SELL":
                proceeds = (order_quantity * final_fill_price) - simulated_commission
                self.cash += proceeds
                
                new_total_qty = current_qty - order_quantity

                if current_qty > 0: # Was long
                    qty_sold_from_long = min(order_quantity, current_qty)
                    qty_going_short = order_quantity - qty_sold_from_long

                    # P&L for sold long part is implicitly realized
                    if new_total_qty < 0: # Flipped to short
                        self.positions[symbol]['cost_basis'] = final_fill_price # Avg entry price for the new short shares
                    elif new_total_qty == 0: # Flattened
                        self.positions[symbol]['cost_basis'] = 0.0
                    else: # Reduced long position
                        # Cost basis of remaining long shares doesn't change
                        pass
                else: # Was flat or short (opening new short or increasing short)
                    if new_total_qty < 0: # Avoid division by zero; abs(current_qty) for old short value
                        # (abs(current_qty) * current_cost_basis) is total value sold short previously
                        # (order_quantity * final_fill_price) is value of new shares shorted
                        # abs(new_total_qty) is total shares shorted
                        new_cost_basis = (abs(current_qty) * current_cost_basis + order_quantity * final_fill_price) / abs(new_total_qty)
                        self.positions[symbol]['cost_basis'] = new_cost_basis
                    else: # Should not happen if current_qty <=0 and order_qty > 0 leading to new_total_qty > 0
                          # This would mean selling while flat/short resulted in a long position, which is wrong.
                          # Unless order_quantity was negative, but we defined it as positive.
                        logger.error(f"Unexpected new_total_qty {new_total_qty} in SELL logic for {symbol}")
                        self.positions[symbol]['cost_basis'] = 0.0


                self.positions[symbol]['quantity'] = new_total_qty
                if new_total_qty == 0:
                     self.positions[symbol]['cost_basis'] = 0.0

            logger.info(f"After {direction} {symbol}: Qty={self.positions[symbol]['quantity']}, CostBasis={self.positions[symbol]['cost_basis']:.2f}, Cash={self.cash:.2f}")

            # Remove settled order
            if order_id in self._pending_orders:
                del self._pending_orders[order_id]
            
            # Log portfolio state
            non_zero_positions = {
                sym: {"qty": pos['quantity'], "cb": f"{pos['cost_basis']:.2f}"}
                for sym, pos in self.positions.items() if pos['quantity'] != 0
            }
            logger.info(f"Post-Settlement Summary: Cash: ${self.cash:.2f}, Total Value: ${self.get_total_portfolio_value():.2f}")
            logger.info(f"Positions: {non_zero_positions or 'No open positions.'}")
            if self._pending_orders:
                logger.info(f"Pending Orders ({len(self._pending_orders)}): "
                            f"{[f'{o.direction} {o.quantity} {o.symbol}' for o in self._pending_orders.values()]}")
            else:
                logger.info("Pending Orders: None.")

        self._last_market_timestamp = current_timestamp