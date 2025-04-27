from trader.base.data_source import IDataSource
from trader.base.event_engine import EventEngine
from trader.base.event import MarketEvent
from typing import List, Optional
import pandas as pd
from datetime import datetime

import logging
logger = logging.getLogger(__name__)

class AkshareDataSource(IDataSource):
    def __init__(self, event_engine: EventEngine, symbols: List[str],
                 start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None):
        super().__init__(event_engine, symbols, start_date, end_date)
        logger.info(f"{self.__class__.__name__} 初始化，关注符号: {self.symbols}, "
                    f"日期范围: {start_date} to {end_date}")
        self._data = None
        
    def _prepare_data(self):
        # 准备数据的逻辑，这里可以是从数据库、API 等获取数据的逻辑
        result_list = []
        start_date_str = self.start_date.strftime('%Y%m%d') if self.start_date else None
        end_date_str = self.end_date.strftime('%Y%m%d') if self.end_date else None

        for symbol in self.symbols:
           df = ak.fund_etf_hist_em(symbol=symbol, start_date=start_date_str, end_date=end_date_str, adjust="hfq")
           logger.info(f"获取到 {symbol} 的数据: {df.head()}")
           df = df[["日期", "开盘", "收盘", "最高", "最低", "成交量"]]
           df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
           df["timestamp"] = pd.to_datetime(df["timestamp"])
           df['symbol'] = symbol
           result_list.append(df)
           
        self._data = pd.concat(result_list)

    def _generate_events(self):
        logger.info("开始生成 MarketEvent 数据流...")
        if self._data.empty:
             logger.warning("无法生成事件流，因为准备好的数据为空。")
             return # Empty generator will be returned implicitly

        for index, row in self._data.iterrows():
            ts = row['timestamp']
            if not isinstance(ts, datetime):
                try:
                    ts = pd.to_datetime(ts)
                except Exception as e:
                    logger.error(f"无法转换时间戳 (行 {index}): {e}")
                    continue

            if self.start_date and ts < self.start_date: continue
            if self.end_date and ts > self.end_date: continue
            
            event = MarketEvent(
                timestamp=ts,
                symbol=row['symbol'],
                open_price=float(row['open']),
                high_price=float(row['high']),
                low_price=float(row['low']),
                close_price=float(row['close']),
                volume=float(row['volume'])
            )
            yield event
        logger.info("MarketEvent 数据流生成结束。")
           
