from abc import ABC, abstractmethod
import pandas as pd
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from trader.base.event import Event, MarketEvent, SignalEvent, FillEvent, OrderEvent, TimerEvent
from trader.base.event_engine import EventEngine
from trader.base.data_handler import DataHandler
from trader.base.portfolio import Position, PortfolioManager

logger = logging.getLogger(__name__)

class Strategy(ABC):
    def __init__(self,
                 strategy_id: str,
                 symbols: List[str],
                 event_engine: EventEngine,
                 data_handler: DataHandler,
                 portfolio_manager: Optional[PortfolioManager] = None,
                 **kwargs):
        self.strategy_id = strategy_id
        self.symbols = symbols
        self.event_engine = event_engine
        self.data_handler = data_handler
        self.portfolio_manager = portfolio_manager

        self._params = {}
        self._params.update(kwargs)
        self._validate_parameters()

        if self.portfolio_manager is None:
             logger.warning(f"策略 '{self.strategy_id}' 未提供 PortfolioManager 实例。"
                            f" 持仓查询和智能下单功能将受限或无法使用。")

        self._active = True
        logger.info(f"策略 '{self.strategy_id}' 初始化，关注符号: {self.symbols}, 参数: {self._params}")

    # --- 核心事件分发器 (Adjusted to be async) ---
    async def handle_event(self, event: Event): # <-- Changed to async def
        if not self.is_active():
            # logger.debug(f"策略 '{self.strategy_id}' 未激活，跳过事件处理。") # Optional debug log
            return

        try:
            # 根据事件类型分派到具体的处理方法
            if event.event_type == MarketEvent.event_type:
                market_event: MarketEvent = event
                if market_event.symbol in self.symbols:
                    await self.on_market_data(market_event) # <-- Await the call
            elif event.event_type == TimerEvent.event_type:
                await self.on_timer(event) # <-- Await the call
            elif event.event_type == FillEvent.event_type:
                fill_event: FillEvent = event
                await self.on_fill(fill_event) # <-- Await the call
            elif event.event_type == OrderEvent.event_type:
                 order_event: OrderEvent = event
                 await self.on_order_status(order_event) # <-- Await the call
            elif event.event_type == SignalEvent.event_type:
                 signal_event: SignalEvent = event
                 await self.on_signal(signal_event) # <-- Await the call
            else:
                await self.on_other_event(event) # <-- Await the call

        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 处理事件 {event.event_type} 时发生错误: {e}", exc_info=True)


    # --- 用户需要覆盖的具体事件处理方法 (remain the same - can be async or sync) ---
    # Note: If you implement any of these in a subclass and need to use 'await',
    # you must define them as 'async def'.
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
        if symbol not in self.symbols:
            logger.warning(f"策略 '{self.strategy_id}' 请求非关注符号 '{symbol}' 的历史数据。")
        try:
            return self.data_handler.get_latest_bars(symbol, N=N)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 获取历史数据 ({symbol}, N={N}) 时出错: {e}", exc_info=False)
            return None

    def get_position(self, symbol: str) -> Optional[Position]:
        if self.portfolio_manager is None:
            logger.error(f"策略 '{self.strategy_id}' 无法查询持仓：PortfolioManager 未设置。")
            return None
        try:
            return self.portfolio_manager.get_position(symbol)
        except Exception as e:
            logger.error(f"策略 '{self.strategy_id}' 查询持仓 ({symbol}) 时出错: {e}", exc_info=False)
            return None

    def get_position_size(self, symbol: str) -> float:
        position = self.get_position(symbol)
        return position.quantity if position else 0.0

    def is_invested(self, symbol: str) -> bool:
        return self.get_position_size(symbol) != 0.0

    def get_all_positions(self) -> Dict[str, Position]:
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
        timestamp = datetime.now()
        latest_bar = self.data_handler.get_current_bar(symbol)
        if latest_bar is not None:
             bar_time = getattr(latest_bar, 'name', None)
             if bar_time is None:
                 bar_time = getattr(latest_bar, 'timestamp', None)
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
        self._create_order(symbol, order_type, 'BUY', quantity, limit_price, order_ref)

    def sell(self, symbol: str, quantity: float, order_type: str = 'MKT', limit_price: Optional[float] = None, order_ref: Optional[str] = None):
        self._create_order(symbol, order_type, 'SELL', quantity, limit_price, order_ref)

    # --- 状态与生命周期 (remain the same) ---
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

    # --- 注册方法 (Adjusted for Async EventEngine) ---
    def register_event_listeners(self):
        """
        向事件引擎注册策略的事件处理方法。
        使用 register_general 注册 handle_event 监听所有事件。
        """
        # Remove the old registration method if it was like this:
        # self.event_engine.register(event_type=None, handler=self.handle_event)

        # Use the new register_general method
        self.event_engine.register_general(self.handle_event) # <-- Use register_general
        logger.info(f"策略 '{self.strategy_id}' 的 handle_event 已注册为通用事件监听器。")

    # --- 参数校验与访问 ---
    def _validate_parameters(self):
        """
        参数校验钩子方法。子类可以覆盖此方法来检查 self._params 中的参数。
        """
        pass

    @property
    def params(self) -> dict:
        """对外暴露的参数接口 (只读副本)"""
        return self._params.copy()