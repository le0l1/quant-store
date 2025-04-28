import pandas as pd
from collections import defaultdict
from typing import List, Optional, Dict

from trader.base.event import MarketEvent, Event
from trader.base.event_engine import EventEngine

class DataHandler:
    def __init__(self, event_engine: EventEngine, symbols: List[str], max_bars: int = 1000):
        self.event_engine = event_engine
        self.symbols = symbols
        self.max_bars = max_bars if max_bars and max_bars > 0 else None
        self._data_frames: Dict[str, pd.DataFrame] = {symbol: pd.DataFrame() for symbol in symbols}
        
        # Register to handle market events
        self.event_engine.register(MarketEvent.event_type, self.on_event)

    def on_event(self, event: Event):
        if event.event_type == MarketEvent.event_type:
            market_event = event
            symbol = market_event.symbol
            if symbol in self.symbols:
                new_bar_data = {
                    'open': market_event.open_price,
                    'high': market_event.high_price,
                    'low': market_event.low_price,
                    'close': market_event.close_price,
                    'volume': market_event.volume
                }
                new_bar_df = pd.DataFrame(new_bar_data, index=[pd.Timestamp(market_event.timestamp)])
                self._data_frames[symbol] = pd.concat([self._data_frames[symbol], new_bar_df])
                
                # Trim data if max_bars is set
                if self.max_bars and len(self._data_frames[symbol]) > self.max_bars:
                    self._data_frames[symbol] = self._data_frames[symbol].iloc[-self.max_bars:]

    def get_latest_bars(self, symbol: str, N: int = 1) -> Optional[pd.DataFrame]:
        if symbol not in self.symbols or self._data_frames[symbol].empty or N <= 0:
            return None
        df = self._data_frames[symbol]
        return df.iloc[-N:] if N <= len(df) else None

    def get_current_bar(self, symbol: str) -> Optional[pd.Series]:
        latest_bars = self.get_latest_bars(symbol, N=1)
        return latest_bars.iloc[0] if latest_bars is not None and not latest_bars.empty else None