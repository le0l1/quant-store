import logging
from typing import List, Optional, Dict
from trader.base.event import Event, MarketEvent, OrderEvent, FillEvent
from trader.base.event_engine import EventEngine
from trader.base.execution import Execution

logger = logging.getLogger(__name__)

class BacktestExecution(Execution):
    def __init__(self,
                 event_engine: EventEngine,
                 commission_rate: float = 0.0, # Default to 0
                 slippage_per_trade: float = 0.0 # Default to 0
                 ):
        """
        初始化回测执行处理器。
        """
        # Store specific config
        self.commission_rate = commission_rate
        self.slippage_per_trade = slippage_per_trade

        # Internal state for pending orders remains
        self._pending_orders: Dict[str, List[OrderEvent]] = {}

        # Call super() AFTER setting internal state but BEFORE registering listeners
        super().__init__(event_engine) # This calls self._register_listeners()

        logger.info(f"BacktestExecutionHandler 初始化: 佣金率={commission_rate}, 滑点={slippage_per_trade}")

    # Implement the mandatory handle_order method
    def handle_order(self, order_event: OrderEvent):
        symbol = order_event.symbol

        if symbol not in self._pending_orders:
            self._pending_orders[symbol] = []
        self._pending_orders[symbol].append(order_event)
        logger.info(f"[{self.__class__.__name__}] 新订单到达 {symbol} @ {order_event.timestamp}: {order_event}")

    # Implement the optional handle_market_data method
    def handle_market_data(self, market_event: MarketEvent):
        symbol = market_event.symbol
        timestamp = market_event.timestamp

        if symbol not in self._pending_orders or not self._pending_orders[symbol]:
            return # No pending orders for this symbol

        logger.info(f"[{self.__class__.__name__}] 市场数据到达 {symbol} @ {timestamp}, "
                     f"检查待处理订单 ({len(self._pending_orders[symbol])}个)...")

        orders_to_process = self._pending_orders.pop(symbol) # Get and remove list atomically

        for order in orders_to_process:
            # --- Simulation Logic (same as before) ---
            if order.order_type == 'MKT' or order.order_type == 'LMT': # Simplified LMT
                fill_price_base = market_event.open_price
                slippage_amount = fill_price_base * self.slippage_per_trade
                fill_price_adjusted = 0.0
                if order.direction == 'BUY':
                    fill_price_adjusted = fill_price_base + slippage_amount
                elif order.direction == 'SELL':
                    fill_price_adjusted = fill_price_base - slippage_amount
                else: logger.error(f"未知订单方向: {order.direction}"); continue

                if order.order_type == 'LMT' and order.limit_price is not None:
                     if order.direction == 'BUY' and fill_price_adjusted > order.limit_price:
                         logger.warning(f"LMT BUY 订单未成交: 成交价 ${fill_price_adjusted:.4f} > 限价 ${order.limit_price:.4f}. Order: {order}")
                         continue # Skip fill generation
                     elif order.direction == 'SELL' and fill_price_adjusted < order.limit_price:
                         logger.warning(f"LMT SELL 订单未成交: 成交价 ${fill_price_adjusted:.4f} < 限价 ${order.limit_price:.4f}. Order: {order}")
                         continue # Skip fill generation


                trade_value = order.quantity * fill_price_adjusted
                commission = trade_value * self.commission_rate

                fill_event = FillEvent(
                    timestamp=timestamp, symbol=symbol, exchange="BACKTEST",
                    order_ref=order.order_ref,
                    fill_id=f"FILL-{order.order_ref}-{timestamp.timestamp():.0f}",
                    direction=order.direction, fill_quantity=order.quantity,
                    fill_price=fill_price_adjusted, commission=commission,
                    slippage=abs(fill_price_adjusted - fill_price_base)
                )
                logger.info(f"<== [{self.__class__.__name__}] 订单执行完成 (Fill): {fill_event.direction} {fill_event.fill_quantity} "
                            f"{fill_event.symbol} @ ${fill_event.fill_price:.4f} (Comm: ${fill_event.commission:.4f}, Slippage: ${fill_event.slippage:.4f}) "
                            f"(Orig Ref: {fill_event.order_ref})")
                self.event_engine.put(fill_event)
            else:
                logger.warning(f"未支持的订单类型 '{order.order_type}'，订单被忽略: {order}")