import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from trader.base import IStrategy

# --- 示例策略：适配新的基类 ---
class MovingAverageCrossStrategy(IStrategy):
    """
    移动平均线交叉策略示例，适配 Revision 2 的 IStrategy。
    """
    # 策略参数声明
    def _validate_parameters(self):
        short_window = self.params.get('short_window') # Use .get for safety
        long_window = self.params.get('long_window')
        if short_window is None or long_window is None:
            raise ValueError("Missing required parameter(s): 'short_window' or 'long_window'")
        if short_window >= long_window:
            raise ValueError("Short window must be smaller than long window")


    def on_market_data(self, event):
        symbol = event.symbol
        signal_time = event.timestamp
        # --- 1. 获取历史数据 (使用新方法) ---
        short_window = self.params.get('short_window')
        long_window = self.params.get('long_window')

        required_bars = long_window + 1
        bars_df = self.get_history(symbol, N=required_bars) # 使用基类方法


        if bars_df is None or len(bars_df) < required_bars: return

        # --- 2. 计算指标 ---
        try:
            close_prices = bars_df['close'].astype(float)
            short_sma = close_prices.rolling(window=short_window).mean()
            long_sma = close_prices.rolling(window=long_window).mean()
            current_short = short_sma.iloc[-1]
            previous_short = short_sma.iloc[-2]
            current_long = long_sma.iloc[-1]
            previous_long = long_sma.iloc[-2]
        except Exception: return # 简化错误处理

        if pd.isna(previous_short) or pd.isna(previous_long) or pd.isna(current_short) or pd.isna(current_long): return

        # --- 3. 决策逻辑 (使用持仓查询) ---
        current_position_size = self.get_position_size(symbol) # 使用基类方法查询持仓

        # 买入条件: 上穿 & 当前无仓位
        if previous_short <= previous_long and current_short > current_long and current_position_size == 0:
            # 决定买入数量 (示例：固定数量或基于资金比例)
            quantity_to_buy = 100 # 示例：固定买入 100 股
            logger.info(f"==> [{self.strategy_id}-{symbol}] 满足 [买入] 条件 @ {signal_time} (Short {current_short:.2f} > Long {current_long:.2f})")
            self.buy(symbol, quantity_to_buy) # 使用基类方法下单

        # 平多仓条件: 下穿 & 当前持有多仓
        elif previous_short >= previous_long and current_short < current_long and current_position_size > 0:
            logger.info(f"==> [{self.strategy_id}-{symbol}] 满足 [平多仓] 条件 @ {signal_time} (Short {current_short:.2f} < Long {current_long:.2f})")
            # 平掉所有持仓 (quantity=None)
            self.close_long(symbol) # 使用基类方法平仓