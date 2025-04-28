# portfolio.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Tuple
import pandas as pd # For equity curve potentially
import logging

from trader.base.event import Event, FillEvent, MarketEvent, PortfolioUpdateEvent
from trader.base.event_engine import EventEngine

logger = logging.getLogger(__name__)

@dataclass
class Position:
    """Represents the holding of a specific asset."""
    symbol: str
    quantity: float = 0.0           # Current holding quantity (can be negative for shorts later)
    average_price: float = 0.0      # Average entry price of the current holding
    last_update_time: Optional[datetime] = None # Time of the last fill affecting this position
    market_price: float = 0.0       # Last known market price for Mark-to-Market
    market_value: float = 0.0       # Current market value (quantity * market_price)
    unrealized_pnl: float = 0.0     # Profit/Loss if position were closed now

# --- Portfolio Manager Interface ---
class PortfolioManager(ABC):
    """
    投资组合管理器接口 (抽象基类)。
    """
    def __init__(self, event_engine: EventEngine, initial_capital: float):
        self.event_engine = event_engine
        self.initial_capital = initial_capital
        # Register necessary event handlers
        self.register_event_handler(event_engine)
        logger.info(f"{self.__class__.__name__} 初始化。")

    def register_event_handler(self, engine: EventEngine):
        """注册需要监听的事件。"""
        engine.register(FillEvent.event_type, self.on_event)
        engine.register(MarketEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 已注册监听 {FillEvent.event_type} 和 {MarketEvent.event_type}。")

    @abstractmethod
    def on_event(self, event: Event):
        """处理事件的核心入口。"""
        pass

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定标的的持仓信息。"""
        pass

    @abstractmethod
    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有当前持仓。"""
        pass

    @abstractmethod
    def get_cash(self) -> float:
        """获取当前可用现金。"""
        pass

    @abstractmethod
    def get_current_holdings_value(self) -> float:
        """获取当前所有持仓的总市值。"""
        pass

    @abstractmethod
    def get_current_equity(self) -> float:
        """获取当前总权益 (现金 + 持仓市值)。"""
        pass

    @abstractmethod
    def get_equity_curve(self) -> List[Tuple[datetime, float]]:
        """获取权益曲线的时间序列。"""
        pass