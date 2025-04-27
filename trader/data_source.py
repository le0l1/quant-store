# data_source.py
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from typing import List, Optional, Generator, Tuple

from event import MarketEvent # Import MarketEvent
from event_engine import EventEngine # Needed to put events

logger = logging.getLogger(__name__)

class IDataSource(ABC):
    """
    数据源接口 (抽象基类) - Redesigned。
    提供标准化的初始化和生命周期管理。
    子类需实现 _load_data (用于历史数据) 或 _connect (用于实时数据)
    以及 _generate_events 来产生事件流。
    """
    def __init__(self, event_engine: EventEngine, symbols: List[str],
                 start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None):
        """
        标准化初始化。

        :param event_engine: 事件引擎实例。
        :param symbols: 关注的标的列表。
        :param start_date: 开始日期 (可选, 用于过滤)。
        :param end_date: 结束日期 (可选, 用于过滤)。
        """
        self.event_engine = event_engine
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self._is_active = False
        self._stream_generator: Optional[Generator[MarketEvent, None, None]] = None
        logger.info(f"{self.__class__.__name__} 初始化，关注符号: {self.symbols}, "
                    f"日期范围: {start_date} to {end_date}")
        # Data loading/connection might happen here or be deferred to start()
        # self._prepare_data() # Example internal call

    def start(self):
        """
        启动数据源。
        通常会准备数据流并设置激活状态。
        """
        if not self._is_active:
            logger.info(f"启动 {self.__class__.__name__}...")
            self._prepare_data() # Ensure data is ready
            self._stream_generator = self._generate_events() # Create the generator
            self._is_active = True
            logger.info(f"{self.__class__.__name__} 已启动。")
        else:
            logger.warning(f"{self.__class__.__name__} 已经启动。")

    def stop(self):
        """
        停止数据源。
        清理资源，重置状态。
        """
        if self._is_active:
            logger.info(f"停止 {self.__class__.__name__}...")
            self._stream_generator = None # Discard generator
            self._cleanup() # Allow subclasses for custom cleanup
            self._is_active = False
            logger.info(f"{self.__class__.__name__} 已停止。")
        else:
            logger.warning(f"{self.__class__.__name__} 尚未启动或已停止。")

    def get_stream(self) -> Generator[MarketEvent, None, None]:
        """
        获取事件流生成器。
        如果未启动，返回一个空的生成器。
        """
        if self._is_active and self._stream_generator:
            return self._stream_generator
        else:
            logger.warning(f"尝试获取数据流，但 {self.__class__.__name__} 未激活或生成器未准备好。")
            # Return an empty generator that immediately stops
            def empty_gen():
                if False: yield # pragma: no cover
            return empty_gen()

    @abstractmethod
    def _prepare_data(self):
        """
        【子类实现】准备数据。
        对于历史数据源，这通常涉及加载文件/数据库记录。
        对于实时数据源，可能涉及建立 API 连接和订阅。
        """
        pass

    @abstractmethod
    def _generate_events(self) -> Generator[MarketEvent, None, None]:
        """
        【子类实现】创建并返回实际产生 MarketEvent 的生成器。
        """
        pass

    def _cleanup(self):
        """
        【子类可选覆盖】在 stop() 时执行的清理操作 (如关闭连接)。
        """
        logger.debug(f"{self.__class__.__name__} 执行默认清理。")
        pass # Default implementation does nothing