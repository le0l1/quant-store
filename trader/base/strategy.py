# strategy.py (Revision 2)
from abc import ABC, abstractmethod
import pandas as pd
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

# 从其他模块导入必要的类
from trader.base.event import Event, MarketEvent, SignalEvent, FillEvent, OrderEvent, TimerEvent
from trader.base.event_engine import EventEngine
from trader.base.data_handler import IDataHandler
from trader.base.portfolio import Position # Import the Position dataclass

# --- Placeholder for PortfolioManager Interface ---
# We define a minimal interface here needed by the strategy base class.
# The full PortfolioManager will be more complex.
class IPortfolioManager(ABC):
    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        pass

    @abstractmethod
    def get_all_positions(self) -> Dict[str, Position]:
        pass

    # We can add get_cash, get_total_value etc. later

logger = logging.getLogger(__name__)

# --- 更易用的策略接口 (Revision 2) ---
class IStrategy(ABC):
    """
    策略接口 (抽象基类) - Revision 2。
    包含买入/卖出、历史查询、持仓查询的便捷方法。
    用户继承此类并实现 on_xxx 事件处理方法。
    """
    def __init__(self,
                 strategy_id: str,
                 symbols: List[str],
                 event_engine: EventEngine,
                 data_handler: IDataHandler,
                 # !! PortfolioManager is now expected !!
                 portfolio_manager: Optional[IPortfolioManager] = None):
        """
        初始化策略基类。

        :param strategy_id: 策略的唯一标识符。
        :param symbols: 此策略关注的标的代码列表。
        :param event_engine: 事件引擎实例。
        :param data_handler: 数据处理器实例。
        :param portfolio_manager: 投资组合管理器实例 (对于持仓查询和智能下单至关重要)。
        """
        self.strategy_id = strategy_id
        self.symbols = symbols
        self.event_engine = event_engine
        self.data_handler = data_handler
        self.portfolio_manager = portfolio_manager # Store the portfolio manager

        if self.portfolio_manager is None:
             logger.warning(f"策略 '{self.strategy_id}' 未提供 PortfolioManager 实例。"
                            f" 持仓查询和智能下单功能将受限或无法使用。")

        self._active = True # 策略是否激活
        logger.info(f"策略 '{self.strategy_id}' 初始化，关注符号: {self.symbols}")

    # --- 核心事件分发器 (保持不变) ---
    def handle_event(self, event: Event):
        # ... (代码与 Revision 1 相同: 检查激活状态, 分发到 on_xxx)
        if not self.is_active(): return
        if event.event_type == MarketEvent.event_type:
            market_event: MarketEvent = event
            if market_event.symbol in self.symbols: 
                self.on_market_data(market_event)
        elif event.event_type == TimerEvent.event_type: self.on_timer(event)
        elif event.event_type == FillEvent.event_type:
            fill_event: FillEvent = event
            if fill_event.symbol in self.symbols: self.on_fill(fill_event)
        # ... (其他事件处理 on_order_status, on_signal, on_other_event) ...
        else: self.on_other_event(event)


    # --- 用户需要覆盖的具体事件处理方法 (保持不变) ---
    def on_start(self): pass
    def on_stop(self): pass
    def on_market_data(self, event: MarketEvent): pass
    def on_timer(self, event: TimerEvent): pass
    def on_fill(self, event: FillEvent): pass
    def on_order_status(self, event: OrderEvent): pass
    def on_signal(self, event: SignalEvent): pass
    def on_other_event(self, event: Event): pass

    # --- 新增: 集成数据和持仓查询方法 ---

    def get_history(self, symbol: str, N: int) -> Optional[pd.DataFrame]:
        """
        获取指定标的最近 N 条历史 K 线数据。
        封装了对 data_handler 的调用。

        :param symbol: 标的代码。
        :param N: 需要获取的 K 线数量。
        :return: Pandas DataFrame 或 None。
        """
        if symbol not in self.symbols:
            logger.warning(f"策略 '{self.strategy_id}' 请求非关注符号 '{symbol}' 的历史数据。")
            # return None # 或者允许查询非关注符号？取决于设计哲学
        try:
            return self.data_handler.get_latest_bars(symbol, N=N)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 获取历史数据 ({symbol}, N={N}) 时出错: {e}", exc_info=False)
            return None

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        获取指定标的的当前持仓信息。
        需要 PortfolioManager 支持。

        :param symbol: 标的代码。
        :return: Position 对象或 None。
        """
        if self.portfolio_manager is None:
            logger.error(f"策略 '{self.strategy_id}' 无法查询持仓：PortfolioManager 未设置。")
            return None
        try:
            return self.portfolio_manager.get_position(symbol)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 查询持仓 ({symbol}) 时出错: {e}", exc_info=False)
            return None

    def get_position_size(self, symbol: str) -> float:
        """
        获取指定标的的当前持仓数量。多头为正，空头为负 (如果支持)。
        返回 0 表示无持仓。
        需要 PortfolioManager 支持。
        """
        position = self.get_position(symbol)
        return position.quantity if position else 0.0

    def is_invested(self, symbol: str) -> bool:
        """检查是否持有指定标的的仓位 (数量不为 0)。"""
        return self.get_position_size(symbol) != 0.0

    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有当前持仓。需要 PortfolioManager 支持。"""
        if self.portfolio_manager is None:
            logger.error(f"策略 '{self.strategy_id}' 无法查询所有持仓：PortfolioManager 未设置。")
            return {}
        try:
             return self.portfolio_manager.get_all_positions()
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 查询所有持仓时出错: {e}", exc_info=False)
            return {}


    # --- 新增: 集成买入/卖出方法 ---
    # 这些方法通常会生成 OrderEvent

    def _create_order(self, symbol: str, order_type: str, direction: str, quantity: float,
                     limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        """内部辅助方法，用于创建并发送 OrderEvent。"""
        # 尝试获取合理的时间戳
        timestamp = datetime.now() # 默认用当前时间，回测时可能被覆盖
        latest_bar = self.data_handler.get_current_bar(symbol)
        if latest_bar is not None and hasattr(latest_bar, 'name') and isinstance(latest_bar.name, datetime):
             timestamp = latest_bar.name # 使用最新 bar 的时间戳更适合回测
        else:
             logger.warning(f"[{self.strategy_id}-{symbol}] 无法获取最新 Bar 时间戳，订单时间戳使用当前时间 {timestamp}。")


        if quantity <= 0:
            logger.error(f"[{self.strategy_id}-{symbol}] 尝试创建数量为非正数 ({quantity}) 的订单。已取消。")
            return

        order = OrderEvent(
            timestamp=timestamp,
            symbol=symbol,
            order_type=order_type.upper(),
            direction=direction.upper(),
            quantity=quantity,
            limit_price=limit_price,
            # 自动生成唯一的订单引用，方便追踪
            order_ref=order_ref or f"{self.strategy_id}-{symbol}-{direction}-{timestamp.timestamp():.0f}"
        )
        logger.info(f"==> [{self.strategy_id}-{symbol}] 创建订单: {order.direction} {order.quantity} @ {order.order_type}"
                    f"{' Price ' + str(order.limit_price) if order.limit_price else ''} (Ref: {order.order_ref})")
        self.event_engine.put(order)


    def buy(self, symbol: str, quantity: float, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        """
        发送买入订单。

        :param symbol: 标的代码。
        :param quantity: 要买入的数量 (正数)。
        :param order_type: 'MKT' (默认) 或 'LMT'。
        :param limit_price: 限价单价格 (如果 order_type='LMT')。
        :param order_ref: 自定义订单引用。
        """
        self._create_order(symbol, order_type, 'BUY', quantity, limit_price, order_ref)

    def sell(self, symbol: str, quantity: float, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        """
        发送卖出订单 (可用于平多仓或开空仓)。

        :param symbol: 标的代码。
        :param quantity: 要卖出的数量 (正数)。
        :param order_type: 'MKT' (默认) 或 'LMT'。
        :param limit_price: 限价单价格 (如果 order_type='LMT')。
        :param order_ref: 自定义订单引用。
        """
        self._create_order(symbol, order_type, 'SELL', quantity, limit_price, order_ref)

    def close_long(self, symbol: str, quantity: Optional[float] = None, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        """
        平掉指定标的的多头仓位。

        :param symbol: 标的代码。
        :param quantity: 要平掉的数量。如果为 None (默认)，则平掉所有持仓。
        :param order_type: 'MKT' (默认) 或 'LMT'。
        :param limit_price: 限价单价格。
        :param order_ref: 自定义订单引用。
        """
        current_qty = self.get_position_size(symbol)
        if current_qty <= 0:
            logger.warning(f"[{self.strategy_id}-{symbol}] 尝试平多仓，但当前无多头持仓 (Qty: {current_qty})。")
            return

        qty_to_sell = current_qty if quantity is None else min(abs(quantity), current_qty)

        if qty_to_sell > 0:
            logger.info(f"[{self.strategy_id}-{symbol}] 计划平多仓，当前持仓 {current_qty}，计划卖出 {qty_to_sell}。")
            ref = order_ref or f"{self.strategy_id}-{symbol}-CLOSE_LONG-{datetime.now().timestamp():.0f}"
            self.sell(symbol, qty_to_sell, order_type, limit_price, ref)
        else:
             logger.warning(f"[{self.strategy_id}-{symbol}] 计算出的平多仓数量为 0 或负数 ({qty_to_sell})，取消操作。")

    # (可以类似地添加 close_short 方法，如果框架支持做空)

    # --- 状态与生命周期 (保持不变) ---
    def deactivate(self):
        if self._active:
            self._active = False
            self.on_stop()
            logger.info(f"策略 '{self.strategy_id}' 已停用。")

    def activate(self):
        if not self._active:
            self._active = True
            self.on_start()
            logger.info(f"策略 '{self.strategy_id}' 已激活。")

    def is_active(self) -> bool: return self._active

    # --- 注册方法 (保持不变) ---
    def register_event_listeners(self):
        self.event_engine.register(event_type=None, handler=self.handle_event)
        logger.info(f"策略 '{self.strategy_id}' 的 handle_event 已注册监听所有事件。")