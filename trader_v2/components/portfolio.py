import logging
from datetime import datetime
from typing import Any, Dict, Optional
import uuid
import math
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
        logger.info(f"TradingSystem received SignalEvent: {event}")
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
        logger.info(f"MomentumTradingSystem processing signal: {signal_event}")
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
        
        # Process all pending orders
        orders_to_settle_ids = list(self._pending_orders.keys())
        for order_id in orders_to_settle_ids:
            order = self._pending_orders.get(order_id)
            if not order:
                logger.warning(f"Pending order {order_id} not found.")
                continue
            
            symbol = order.symbol
            quantity = order.quantity
            direction = order.direction
            
            # Get latest price from data_feed instead of MarketEvent
            latest_price = self.data_feed.get_latest_price(symbol)
            if latest_price is None or latest_price <= 0:
                logger.warning(f"Invalid price for {symbol} at {current_timestamp}.")
                continue
                
            base_simulated_price = latest_price
            if base_simulated_price is None or base_simulated_price <= 0:
                logger.warning(f"Invalid price for {symbol} at {current_timestamp}.")
                continue
                
            final_fill_price = base_simulated_price
            slippage_amount = base_simulated_price * self.slippage_percent
            if direction == "BUY":
                final_fill_price += slippage_amount
            elif direction == "SELL":
                final_fill_price -= slippage_amount
            
            if final_fill_price <= 0:
                logger.warning(f"Final price for {symbol} is non-positive ({final_fill_price}).")
                continue
                
            simulated_commission = final_fill_price * quantity * self.commission_percent
            logger.info(f"Settling order {order_id}: {direction} {quantity} of {symbol} "
                        f"at ${final_fill_price:.4f} (Commission: ${simulated_commission:.4f})")
            # Update portfolio state directly
            current_qty = self.get_current_position_quantity(symbol)
            if direction == "BUY":
                cost = (quantity * final_fill_price) + simulated_commission
                if cost > self.cash:
                    logger.error(f"ERROR: Insufficient cash for BUY. Required: ${cost:.2f}, Available: ${self.cash:.2f}.")
                    continue
                self.cash -= cost
                new_total_qty = current_qty + quantity
                if new_total_qty > 0:
                    current_cost_value = current_qty * self.positions[symbol]['cost_basis']
                    fill_cost_value = quantity * final_fill_price
                    new_cost_basis = (current_cost_value + fill_cost_value) / new_total_qty
                    self.positions[symbol]['quantity'] = new_total_qty
                    self.positions[symbol]['cost_basis'] = new_cost_basis
                elif new_total_qty == 0:
                    self.positions[symbol]['quantity'] = 0
                    self.positions[symbol]['cost_basis'] = 0.0
                else:
                    self.positions[symbol]['quantity'] = new_total_qty
                    logger.warning(f"BUY resulted in short position for {symbol}.")
            elif direction == "SELL":
                if quantity > current_qty:
                    quantity = current_qty
                proceeds = (quantity * final_fill_price) - simulated_commission
                self.cash += proceeds
                new_total_qty = current_qty - quantity
                self.positions[symbol]['quantity'] = new_total_qty
                if new_total_qty == 0:
                    self.positions[symbol]['cost_basis'] = 0.0
                elif new_total_qty < 0:
                    self.positions[symbol]['cost_basis'] = 0.0
                    logger.warning(f"SELL resulted in short position for {symbol}.")
                del self.positions[symbol]
            # Remove settled order
            if order_id in self._pending_orders:
                del self._pending_orders[order_id]
            # Log portfolio state
            non_zero_positions = {sym: pos['quantity'] for sym, pos in self.positions.items() if pos['quantity'] != 0}
            logger.info(f"Post-Settlement Summary: Cash: ${self.cash:.2f}, Total Value: ${self.get_total_portfolio_value():.2f}")
            logger.info(f"Positions: {non_zero_positions or 'No open positions.'}")
            if self._pending_orders:
                logger.info(f"Pending Orders ({len(self._pending_orders)}): "
                            f"{[f'{order.direction} {order.quantity} {order.symbol}' for order in self._pending_orders.values()]}")
            else:
                logger.info("Pending Orders: None.")
        self._last_market_timestamp = current_timestamp
