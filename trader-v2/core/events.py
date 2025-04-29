import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

@dataclass
class Event:
    """Base class for all events."""
    # 在基类中定义，并带有默认值
    type: str = field(default="Event") # 给个基础默认值，通常会被子类覆盖
    timestamp: datetime = field(default_factory=datetime.utcnow)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(kw_only=True)
class MarketEvent(Event):
    """Market data event."""
    # 定义子类特有的非默认字段
    symbol: str
    data: Dict[str, Any]

    # 重新定义父类中需要排在后面的带默认值字段
    type: str = "MARKET" # 覆盖父类type，作为默认值


@dataclass(kw_only=True)
class SignalEvent(Event):
    """Trading signal event from Strategy."""
    # 定义子类特有的非默认字段
    symbol: str      # 交易标的
    direction: str   # "LONG", "SHORT", "FLAT"

    # quantity: Optional[int] = None # 数量
    # weight: Optional[float] = None # 占比
    type: str = "SIGNAL"


@dataclass(kw_only=True)
class OrderEvent(Event):
    """Order event from Portfolio to Execution Handler."""
    # 定义子类特有的非默认字段
    symbol: str      # 交易标的
    direction: str   # "BUY", "SELL"
    quantity: int    # 订单数量
    order_type: str  # "MARKET", "LIMIT", "STOP"
    price: Optional[float] = None # 这是带默认值的 Optional 字段，可以放在非默认字段后面

    # 重新定义父类中需要排在后面的带默认值字段
    type: str = "ORDER"


@dataclass(kw_only=True)
class FillEvent(Event):
    """Fill event from Execution Handler."""
    # 定义子类特有的非默认字段
    order_id: str      # 对应的 OrderEvent ID
    symbol: str        # 交易标的
    direction: str     # "BUY", "SELL" (成交方向)
    quantity: int      # 成交数量
    price: float       # 成交价格
    commission: float  # 交易费用

    # 重新定义父类中需要排在后面的带默认值字段
    type: str = "FILL"


@dataclass(kw_only=True)
class HeartbeatEvent(Event):
    """Heartbeat event for system health monitoring or periodic tasks."""
    # HeartbeatEvent 没有新的非默认字段，所以只需要覆盖 type
    type: str = "HEARTBEAT"


@dataclass(kw_only=True)
class BacktestStartEvent(Event):
    """Signals the start of a backtest."""
    # 定义子类特有的非默认字段
    start_time: datetime
    end_time: datetime

    # 重新定义父类中需要排在后面的带默认值字段
    type: str = "BACKTEST_START"


@dataclass(kw_only=True)
class BacktestEndEvent(Event):
    """Signals the end of a backtest."""
    # 定义子类特有的非默认字段
    end_time: datetime

    # 重新定义父类中需要排在后面的带默认值字段
    type: str = "BACKTEST_END"
