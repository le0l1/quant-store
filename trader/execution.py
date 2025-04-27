# execution.py
from abc import ABC, abstractmethod
import logging
from datetime import datetime
from typing import Optional, Dict, List

from event import Event, OrderEvent, FillEvent, MarketEvent
from event_engine import EventEngine

logger = logging.getLogger(__name__)

# --- Execution Handler Interface ---
class IExecutionHandler(ABC):
    """
    执行处理器接口 (抽象基类)。
    负责处理订单事件 (OrderEvent)，并产生相应的成交事件 (FillEvent)。
    """
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        # 注册监听订单事件
        self.register_event_handler(event_engine)
        logger.info(f"{self.__class__.__name__} 初始化完成。")

    def register_event_handler(self, engine: EventEngine):
        """将处理器注册到事件引擎以接收订单事件。"""
        engine.register(OrderEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 已注册到事件引擎处理 {OrderEvent.event_type}。")

    @abstractmethod
    def on_event(self, event: Event):
        """处理事件，主要是订单事件。"""
        pass

    # 可以添加 cancel_order 等其他必要方法接口


# --- Backtest Execution Handler Implementation ---
class BacktestExecutionHandler(IExecutionHandler):
    """
    回测执行处理器。
    模拟订单在历史数据中的执行，考虑滑点和佣金。
    使用下一根 Bar 的开盘价作为大致的成交价。
    """
    def __init__(self,
                 event_engine: EventEngine,
                 commission_rate: float = 0.0001, # 示例佣金率 (e.g., 0.01%)
                 slippage_per_trade: float = 0.0001 # 示例固定滑点 (e.g., 0.01%)
                 ):
        """
        初始化回测执行处理器。

        :param event_engine: 事件引擎实例。
        :param commission_rate: 每笔交易的佣金率 (基于交易金额)。
        :param slippage_per_trade: 每次成交的模拟滑点 (价格单位或百分比，取决于如何应用)。
                                   这里简单假设为价格的百分比。
        """
        super().__init__(event_engine) # 调用父类初始化并注册 OrderEvent 监听

        self.commission_rate = commission_rate
        self.slippage_per_trade = slippage_per_trade

        # --- 状态管理: 存储待处理订单 ---
        # 订单产生后，需要等待下一市场数据来确定成交价
        # Key: symbol, Value: list of pending OrderEvent for that symbol
        self._pending_orders: Dict[str, List[OrderEvent]] = {}

        # 为了获取下一 Bar 的开盘价，还需要监听 MarketEvent
        self.event_engine.register(MarketEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 同时注册监听 {MarketEvent.event_type} 以处理挂单。")

        logger.info(f"BacktestExecutionHandler 初始化: 佣金率={commission_rate}, 滑点={slippage_per_trade}")


    def on_event(self, event: Event):
        """根据事件类型分发处理逻辑。"""
        if event.event_type == OrderEvent.event_type:
            self._handle_new_order(event)
        elif event.event_type == MarketEvent.event_type:
            self._handle_market_data(event)


    def _handle_new_order(self, order_event: OrderEvent):
        """处理新的订单请求，将其加入待处理队列。"""
        symbol = order_event.symbol
        logger.debug(f"[{self.__class__.__name__}] 收到新订单并暂存: {order_event}")

        if symbol not in self._pending_orders:
            self._pending_orders[symbol] = []
        self._pending_orders[symbol].append(order_event)


    def _handle_market_data(self, market_event: MarketEvent):
        """处理市场数据，检查是否有对应标的的待处理订单需要执行。"""
        symbol = market_event.symbol
        timestamp = market_event.timestamp # 成交时间戳是这个新 Bar 的时间

        if symbol in self._pending_orders and self._pending_orders[symbol]:
            logger.debug(f"[{self.__class__.__name__}] 市场数据到达 {symbol} @ {timestamp}, "
                         f"检查待处理订单 ({len(self._pending_orders[symbol])}个)...")

            # 处理该 symbol 的所有挂单
            orders_to_process = self._pending_orders[symbol][:] # 复制列表以安全迭代和修改
            self._pending_orders[symbol] = [] # 清空原列表

            for order in orders_to_process:
                # 假设市价单 (MKT) 使用下一 Bar (即当前 MarketEvent) 的开盘价成交
                # 限价单 (LMT) 的逻辑会更复杂 (检查价格是否触及)
                # 为了简化，我们先假设所有订单都按市价单处理，并使用开盘价

                if order.order_type == 'MKT' or order.order_type == 'LMT': # 简化 LMT 处理
                    fill_price_base = market_event.open_price # 使用新 Bar 的开盘价

                    # 模拟滑点
                    slippage_amount = fill_price_base * self.slippage_per_trade
                    if order.direction == 'BUY':
                        fill_price_adjusted = fill_price_base + slippage_amount # 买入价更高
                    elif order.direction == 'SELL':
                        fill_price_adjusted = fill_price_base - slippage_amount # 卖出价更低
                    else:
                        logger.error(f"未知的订单方向: {order.direction}")
                        continue # 跳过此订单

                    # 模拟佣金
                    trade_value = order.quantity * fill_price_adjusted
                    commission = trade_value * self.commission_rate

                    # 创建成交事件
                    fill_event = FillEvent(
                        timestamp=timestamp, # 成交时间是新 Bar 的时间
                        symbol=symbol,
                        exchange="BACKTEST", # 模拟交易所
                        order_ref=order.order_ref, # 关联原始订单
                        fill_id=f"FILL-{order.order_ref}-{timestamp.timestamp():.0f}", # 创建唯一成交 ID
                        direction=order.direction,
                        fill_quantity=order.quantity, # 假设完全成交
                        fill_price=fill_price_adjusted,
                        commission=commission,
                        slippage=abs(fill_price_adjusted - fill_price_base) # 记录实际滑点值
                    )

                    logger.info(f"<== [{self.__class__.__name__}] 订单执行完成 (Fill): {fill_event.direction} {fill_event.fill_quantity} "
                                f"{fill_event.symbol} @ ${fill_event.fill_price:.4f} (Comm: ${fill_event.commission:.4f}, Slippage: ${fill_event.slippage:.4f}) "
                                f"(Orig Ref: {fill_event.order_ref})")

                    # 将成交事件放入队列
                    self.event_engine.put(fill_event)

                else:
                    logger.warning(f"未支持的订单类型 '{order.order_type}'，订单被忽略: {order}")
                    # 可以选择将未处理的订单放回 pending 列表，或直接丢弃

            # 如果处理后该 symbol 不再有挂单，可以从字典中移除 key (可选)
            if not self._pending_orders[symbol]:
                 del self._pending_orders[symbol]