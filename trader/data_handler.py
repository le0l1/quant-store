# data_handler.py
import pandas as pd
from abc import ABC, abstractmethod
from collections import defaultdict, deque
import logging
from typing import List, Optional, Dict

from event import MarketEvent, Event # Import necessary events
from event_engine import EventEngine # Needed to register handler

logger = logging.getLogger(__name__)

class IDataHandler(ABC):
    """
    数据处理器接口 (抽象基类)。
    负责接收市场数据事件，存储数据，并提供给策略访问。
    """
    def __init__(self, event_engine: EventEngine, symbols: List[str]):
        self.event_engine = event_engine
        self.symbols = symbols
        # 注册自身到事件引擎，监听市场事件
        self.register_event_handler(event_engine)
        logger.info(f"{self.__class__.__name__} 初始化，关注符号: {self.symbols}")

    def register_event_handler(self, engine: EventEngine):
        """将处理器注册到事件引擎以接收市场事件。"""
        engine.register(MarketEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 已注册到事件引擎处理 {MarketEvent.event_type}。")

    @abstractmethod
    def on_event(self, event: Event):
        """处理事件，通常是更新内部数据存储。"""
        pass

    @abstractmethod
    def get_latest_bars(self, symbol: str, N: int = 1) -> Optional[pd.DataFrame]:
        """
        获取指定标的最近的 N 条 K 线数据。

        :param symbol: 标的代码。
        :param N: 需要获取的 K 线数量。
        :return: 包含 K 线数据的 Pandas DataFrame，如果数据不足或标的不存在则返回 None 或空 DataFrame。
                 DataFrame 的索引应该是时间戳，列应包含 open, high, low, close, volume。
        """
        pass

    @abstractmethod
    def get_current_bar(self, symbol: str) -> Optional[pd.Series]:
        """
        获取指定标的当前最新的 K 线数据。

        :param symbol: 标的代码。
        :return: 包含单条 K 线数据的 Pandas Series，如果不存在则返回 None。
        """
        pass


# --- 基础数据处理器实现 ---
class BasicDataHandler(IDataHandler):
    """
    一个基础的数据处理器实现，使用字典存储每个标的的 Pandas DataFrame。
    可以配置存储的最大 K 线数量。
    """
    def __init__(self, event_engine: EventEngine, symbols: List[str], max_bars: int = 1000):
        """
        初始化 BasicDataHandler。

        :param event_engine: 事件引擎实例。
        :param symbols: 需要处理数据的标的列表。
        :param max_bars: 每个标的最大存储的 K 线数量。如果为 0 或 None，则存储所有历史数据（小心内存占用）。
        """
        # 使用字典存储每个 symbol 的数据，值为 deque 或 DataFrame
        # 使用 deque 可以高效地限制长度
        self._data_frames: Dict[str, pd.DataFrame] = {symbol: pd.DataFrame() for symbol in symbols}
        # 或者使用 deque of Series/tuples for memory efficiency if needed
        # self._data_deques: Dict[str, deque] = {symbol: deque(maxlen=max_bars if max_bars else None) for symbol in symbols}

        self.max_bars = max_bars if max_bars and max_bars > 0 else None

        super().__init__(event_engine, symbols) # 调用父类初始化，会完成事件注册
        logger.info(f"BasicDataHandler 初始化，每个符号最多存储 {self.max_bars or '所有'} 条 K 线。")

    def on_event(self, event: Event):
        """处理市场事件，更新对应标的的数据。"""
        if event.event_type == MarketEvent.event_type:
            market_event = event
            symbol = market_event.symbol

            if symbol in self.symbols:
                # 将新 K 线添加到 DataFrame
                new_bar_data = {
                    'open': market_event.open_price,
                    'high': market_event.high_price,
                    'low': market_event.low_price,
                    'close': market_event.close_price,
                    'volume': market_event.volume
                }
                # 使用事件的时间戳作为 DataFrame 的索引
                new_bar_index = pd.Timestamp(market_event.timestamp)
                new_bar_df = pd.DataFrame(new_bar_data, index=[new_bar_index])

                # 追加数据
                # 使用 concat 比 append 更推荐 (append 未来会被移除)
                self._data_frames[symbol] = pd.concat([self._data_frames[symbol], new_bar_df])

                # 如果设置了最大 K 线数，则进行截断
                if self.max_bars and len(self._data_frames[symbol]) > self.max_bars:
                    # 保留最新的 max_bars 条记录
                    self._data_frames[symbol] = self._data_frames[symbol].iloc[-self.max_bars:]

                # logger.debug(f"DataHandler 更新了 {symbol} 的数据，当前共 {len(self._data_frames[symbol])} 条。最新时间: {new_bar_index}")


    def get_latest_bars(self, symbol: str, N: int = 1) -> Optional[pd.DataFrame]:
        """获取指定标的最近的 N 条 K 线数据。"""
        if symbol not in self.symbols:
            logger.warning(f"请求未追踪的符号 '{symbol}' 的数据。")
            return None
        if symbol not in self._data_frames or self._data_frames[symbol].empty:
            # logger.debug(f"符号 '{symbol}' 尚无数据。")
            return None # 或者返回空的 DataFrame: pd.DataFrame()

        df = self._data_frames[symbol]
        if N > len(df):
            # logger.debug(f"请求 {N} 条 K 线，但符号 '{symbol}' 只有 {len(df)} 条可用。")
            return None # 或者返回所有可用数据: df.iloc[-len(df):]
        elif N <= 0:
             logger.warning(f"请求的 K 线数量 N ({N}) 必须为正数。")
             return None

        # 返回最新的 N 条记录
        return df.iloc[-N:]

    def get_current_bar(self, symbol: str) -> Optional[pd.Series]:
        """获取指定标的当前最新的 K 线数据。"""
        latest_bars = self.get_latest_bars(symbol, N=1)
        if latest_bars is not None and not latest_bars.empty:
            # .iloc[0] 因为 get_latest_bars(N=1) 返回的是一个单行 DataFrame
            return latest_bars.iloc[0]
        else:
            return None