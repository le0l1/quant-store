from abc import ABC, abstractmethod
import logging
from datetime import datetime
from typing import Optional, Dict, List

# Import necessary event types
from trader.base.event import Event, OrderEvent, FillEvent, MarketEvent
from trader.base.event_engine import EventEngine

logger = logging.getLogger(__name__)

class Execution(ABC):
    """
    执行处理器接口 (抽象基类) - Redesigned。
    提供事件分发机制，子类需实现核心的 handle_order 方法。
    """
    def __init__(self, event_engine: EventEngine):
        """
        标准化初始化。

        :param event_engine: 事件引擎实例。
        """
        self.event_engine = event_engine
        # 子类负责注册它们需要处理的事件类型
        self._register_listeners()
        logger.info(f"{self.__class__.__name__} 初始化完成。")

    def _register_listeners(self):
        """
        【子类可选覆盖】注册需要监听的事件。
        默认只监听 OrderEvent。
        """
        self.event_engine.register(OrderEvent.event_type, self.on_event)
        self.event_engine.register(MarketEvent.event_type, self.on_event) # Optional, but useful for backtest execution
        logger.info(f"{self.__class__.__name__} 已注册监听 {OrderEvent.event_type}。")

    # --- Concrete event dispatcher ---
    def on_event(self, event: Event):
        """
        统一事件入口，由事件引擎调用。
        根据事件类型分发到具体的 handle_xxx 方法。
        """
        if event.event_type == OrderEvent.event_type:
            self.handle_order(event) # Cast handled within method if needed
        elif event.event_type == MarketEvent.event_type:
            # Only called if subclass registered for MarketEvent
            self.handle_market_data(event)
        else:
            # Handle other potential events if needed
            self.handle_other_event(event)

    # --- Abstract method for core logic ---
    @abstractmethod
    def handle_order(self, order_event: OrderEvent):
        """
        【子类必须实现】处理传入的订单请求的核心方法。
        子类的主要执行逻辑（模拟成交、发送到 API 等）应在此实现。
        """
        pass

    # --- Optional handlers for subclasses ---
    def handle_market_data(self, market_event: MarketEvent):
        """
        【子类可选覆盖】处理市场数据事件。
        主要用于需要市场价格来模拟成交的回测处理器。
        """
        pass # Default implementation does nothing

    def handle_other_event(self, event: Event):
        """
        【子类可选覆盖】处理其他未明确分类的事件。
        """
        pass # Default implementation does nothing

    # --- Optional: Lifecycle methods ---
    def start(self):
        """【子类可选覆盖】启动执行处理器（例如，连接到交易 API）。"""
        logger.info(f"启动 {self.__class__.__name__}。")
        pass

    def stop(self):
        """【子类可选覆盖】停止执行处理器（例如，断开 API 连接）。"""
        logger.info(f"停止 {self.__class__.__name__}。")
        pass

    # --- Optional: Other actions ---
    # def cancel_order(self, order_ref: str):
    #     """【子类可选实现】取消一个已发送的订单。"""
    #     logger.warning(f"{self.__class__.__name__} 未实现 cancel_order。")
    #     pass

