# event.py (修正后)
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Dict # Import Dict

# --- Base Event ---
@dataclass
class Event:
    """
    所有事件类的基类。
    """

# --- Market Events ---
@dataclass
class MarketEvent(Event):
    timestamp: datetime
    symbol: str
    # === 有默认值的字段在后 ===
    event_type: str = "MARKET" 
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    volume: float = 0.0


# --- Signal Events ---
@dataclass
class SignalEvent(Event):
    """
    表示策略产生的交易信号事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    symbol: str
    signal_type: str
    # 有默认值的字段在后
    event_type: str = "SIGNAL"
    strength: float = 1.0
    target_price: Optional[float] = None

# --- Order Events ---
@dataclass
class OrderEvent(Event):
    """
    表示创建交易委托的请求事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    symbol: str
    order_type: str
    direction: str
    quantity: float
    # 有默认值的字段在后
    event_type: str = "ORDER"
    limit_price: Optional[float] = None
    order_ref: Optional[str] = None

# --- Fill Events ---
@dataclass
class FillEvent(Event):
    """
    表示订单成交的回报事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    symbol: str
    direction: str
    fill_quantity: float
    fill_price: float
    # 有默认值的字段在后
    event_type: str = "FILL"
    exchange: Optional[str] = None
    order_ref: Optional[str] = None
    fill_id: Optional[str] = None
    commission: float = 0.0
    slippage: float = 0.0

# --- Timer Events ---
@dataclass
class TimerEvent(Event):
    """
    表示定时器触发的事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    # 有默认值的字段在后
    event_type: str = "TIMER"

# --- System Events ---
@dataclass
class SystemEvent(Event):
    """
    表示系统级别的事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    message: str
    # 有默认值的字段在后
    event_type: str = "SYSTEM"

# --- Portfolio Update Event ---
@dataclass
class PortfolioUpdateEvent(Event):
    """
    表示投资组合状态更新的事件。
    """
    # 无默认值的字段在前
    timestamp: datetime
    portfolio_id: str
    total_value: float
    cash: float
    positions: Dict[str, Any] # 使用 Dict 而不是 dict
    # 有默认值的字段在后
    event_type: str = "PORTFOLIO_UPDATE"
    pnl: Optional[float] = None # 明确设为 Optional 或给默认值