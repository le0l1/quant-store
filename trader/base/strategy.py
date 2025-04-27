# strategy.py (Revision 2 - Adjusted __init__)
from abc import ABC, abstractmethod
import pandas as pd
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

# ... (imports remain the same) ...
from trader.base.event import Event, MarketEvent, SignalEvent, FillEvent, OrderEvent, TimerEvent
from trader.base.event_engine import EventEngine
from trader.base.data_handler import IDataHandler
from trader.base.portfolio import Position # Import the Position dataclass

# --- Placeholder for PortfolioManager Interface (remains the same) ---
class IPortfolioManager(ABC):
    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        pass

    @abstractmethod
    def get_all_positions(self) -> Dict[str, Position]:
        pass

logger = logging.getLogger(__name__)

# --- 更易用的策略接口 (Revision 2 - Adjusted __init__) ---
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
                 portfolio_manager: Optional[IPortfolioManager] = None,
                 **kwargs): # <--- ADDED **kwargs HERE
        """
        初始化策略基类

        :param strategy_id: 策略的唯一标识符
        :param symbols: 关注的标的代码列表
        :param event_engine: 事件引擎实例
        :param data_handler: 数据处理器实例
        :param portfolio_manager: 投资组合管理器实例
        :param kwargs: 策略特定的配置参数 (e.g., window_size, risk_limit)
        """
        # Store core components
        self.strategy_id = strategy_id
        self.symbols = symbols
        self.event_engine = event_engine
        self.data_handler = data_handler
        self.portfolio_manager = portfolio_manager

        # --- ADJUSTED PARAMETER HANDLING ---
        # Initialize internal parameter storage
        self._params = {}

        # Optional: Capture default parameters defined as class attributes
        # in the specific strategy implementation (StrategyCls).
        # Be careful this doesn't pick up unwanted attributes.
        # It's often cleaner for StrategyCls to define defaults in its own __init__
        # or rely solely on passed kwargs. Let's keep it simple for now
        # and primarily rely on passed kwargs. You can uncomment/refine this
        # if you need class-level defaults.
        # default_params = {
        #     attr: getattr(self.__class__, attr)
        #     for attr in dir(self.__class__)
        #     if not attr.startswith('_') and not callable(getattr(self.__class__, attr))
        #        and attr not in ['strategy_id', 'symbols', 'event_engine', 'data_handler', 'portfolio_manager', '_active', '_params', 'params'] # Exclude known base attributes
        # }
        # self._params.update(default_params)

        # Update and override with parameters passed during instantiation
        self._params.update(kwargs) # <--- STORE kwargs HERE

        # Now validate the parameters *after* they've been stored
        self._validate_parameters()
        # --- END ADJUSTED PARAMETER HANDLING ---


        if self.portfolio_manager is None:
             logger.warning(f"策略 '{self.strategy_id}' 未提供 PortfolioManager 实例。"
                            f" 持仓查询和智能下单功能将受限或无法使用。")

        self._active = True # 策略是否激活
        logger.info(f"策略 '{self.strategy_id}' 初始化，关注符号: {self.symbols}, 参数: {self._params}") # Log params

    # --- 核心事件分发器 (remains the same) ---
    def handle_event(self, event: Event):
        # ... (code remains the same) ...
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

    # --- 用户需要覆盖的具体事件处理方法 (remain the same) ---
    def on_start(self): pass
    def on_stop(self): pass
    def on_market_data(self, event: MarketEvent): pass
    def on_timer(self, event: TimerEvent): pass
    def on_fill(self, event: FillEvent): pass
    def on_order_status(self, event: OrderEvent): pass
    def on_signal(self, event: SignalEvent): pass
    def on_other_event(self, event: Event): pass

    # --- 集成数据和持仓查询方法 (remain the same) ---
    def get_history(self, symbol: str, N: int) -> Optional[pd.DataFrame]:
        # ... (code remains the same) ...
        if symbol not in self.symbols:
            logger.warning(f"策略 '{self.strategy_id}' 请求非关注符号 '{symbol}' 的历史数据。")
        try:
            return self.data_handler.get_latest_bars(symbol, N=N)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 获取历史数据 ({symbol}, N={N}) 时出错: {e}", exc_info=False)
            return None

    def get_position(self, symbol: str) -> Optional[Position]:
        # ... (code remains the same) ...
        if self.portfolio_manager is None:
            logger.error(f"策略 '{self.strategy_id}' 无法查询持仓：PortfolioManager 未设置。")
            return None
        try:
            return self.portfolio_manager.get_position(symbol)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 查询持仓 ({symbol}) 时出错: {e}", exc_info=False)
            return None

    def get_position_size(self, symbol: str) -> float:
        # ... (code remains the same) ...
        position = self.get_position(symbol)
        return position.quantity if position else 0.0

    def is_invested(self, symbol: str) -> bool:
        # ... (code remains the same) ...
        return self.get_position_size(symbol) != 0.0

    def get_all_positions(self) -> Dict[str, Position]:
        # ... (code remains the same) ...
        if self.portfolio_manager is None:
            logger.error(f"策略 '{self.strategy_id}' 无法查询所有持仓：PortfolioManager 未设置。")
            return {}
        try:
             return self.portfolio_manager.get_all_positions()
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 查询所有持仓时出错: {e}", exc_info=False)
            return {}

    # --- 集成买入/卖出方法 (remain the same) ---
    def _create_order(self, symbol: str, order_type: str, direction: str, quantity: float,
                     limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        # ... (code remains the same) ...
        timestamp = datetime.now() # 默认用当前时间，回测时可能被覆盖
        latest_bar = self.data_handler.get_current_bar(symbol)
        if latest_bar is not None:
             # Try to get timestamp from bar index (assuming it's datetime-like)
             bar_time = getattr(latest_bar, 'name', None) # For Series
             if bar_time is None:
                 bar_time = getattr(latest_bar, 'timestamp', None) # Check common attributes
             if isinstance(bar_time, (datetime, pd.Timestamp)):
                 timestamp = bar_time
             else:
                 logger.warning(f"[{self.strategy_id}-{symbol}] 最新 Bar 时间戳类型未知 ({type(bar_time)})，订单时间戳使用当前时间 {timestamp}。")
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
            order_ref=order_ref or f"{self.strategy_id}-{symbol}-{direction}-{timestamp.timestamp():.0f}"
        )
        logger.info(f"==> [{self.strategy_id}-{symbol}] 创建订单: {order.direction} {order.quantity} @ {order.order_type}"
                    f"{' Price ' + str(order.limit_price) if order.limit_price else ''} (Ref: {order.order_ref})")
        self.event_engine.put(order)


    def buy(self, symbol: str, quantity: float, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        # ... (code remains the same) ...
        self._create_order(symbol, order_type, 'BUY', quantity, limit_price, order_ref)

    def sell(self, symbol: str, quantity: float, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        # ... (code remains the same) ...
        self._create_order(symbol, order_type, 'SELL', quantity, limit_price, order_ref)

    def close_long(self, symbol: str, quantity: Optional[float] = None, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        # ... (code remains the same) ...
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


    # --- 状态与生命周期 (remain the same) ---
    def deactivate(self):
        # ... (code remains the same) ...
        if self._active:
            self._active = False
            self.on_stop()
            logger.info(f"策略 '{self.strategy_id}' 已停用。")

    def activate(self):
        # ... (code remains the same) ...
        if not self._active:
            self._active = True
            self.on_start()
            logger.info(f"策略 '{self.strategy_id}' 已激活。")

    def is_active(self) -> bool: return self._active

    # --- 注册方法 (remain the same) ---
    def register_event_listeners(self):
        # ... (code remains the same) ...
        self.event_engine.register(event_type=None, handler=self.handle_event)
        logger.info(f"策略 '{self.strategy_id}' 的 handle_event 已注册监听所有事件。")

    # --- 参数校验与访问 ---
    def _validate_parameters(self):
        """
        参数校验钩子方法。子类可以覆盖此方法来检查 self._params 中的参数。
        例如:
        if 'window' not in self._params or not isinstance(self._params['window'], int) or self._params['window'] <= 0:
            raise ValueError("Parameter 'window' is missing, not an integer, or not positive.")
        """
        pass # Base class does no validation by default

    @property
    def params(self) -> dict:
        """对外暴露的参数接口 (只读副本)"""
        # Return a copy to prevent external modification of internal state
        return self._params.copy()
