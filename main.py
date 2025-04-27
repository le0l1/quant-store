import logging
from datetime import datetime
import asyncio

# 从框架导入 Backtester
from trader.backtest.backtester import Backtester
# 从策略文件导入用户的策略类
from strategy import MovingAverageCrossStrategy
# 从数据源文件导入使用的数据源类
from trader.data_source.csv_data_source import CSVDataSource


# --- 用户配置 (简化版) ---
config = {
    # === 核心配置 (必须) ===
    'initial_capital': 100000.0,
    'start_date': datetime(2023, 1, 1),
    'end_date': datetime(2023, 12, 31),
    'symbols': ['513500'],

    # === 数据源配置 (必须) ===
    'data_source_config': {
        'class': CSVDataSource,  # 注意：此处无需修改类名，只需确认导入路径正确,
        'params': {
            'file_path': 'etf.csv'
        }
    },

    # === 策略配置 (必须) ===
    'strategy_config': {
        'class': MovingAverageCrossStrategy,  # 保持类名不变，确保导入路径已修正,
        'params': {
            'short_window': 15,
            'long_window': 45
        }
    },

    # === 执行配置 (可选, 使用默认值或覆盖) ===
    'execution_config': {
        # 'class': MyCustomExecutionHandler, # 取消注释以使用自定义类
        'commission_rate': 0.0005,         # 覆盖默认佣金 (默认是 0)
        'slippage_per_trade': 0.0002      # 覆盖默认滑点 (默认是 0)
    },

    # === 其他可选配置 (只有在需要自定义时才添加) ===
    # 'portfolio_config': {'class': MyPortfolio, 'params': {...}},
    # 'data_handler_config': {'class': MyDataHandler, 'params': {...}},
    # 'performance_analyzer_config': {'class': MyPerfAnalyzer, 'params': {...}},
    # 'log_level': 'DEBUG' # 覆盖默认日志级别 INFO
}


# --- 执行回测 ---
if __name__ == "__main__":
    # (Dummy data generation code can remain here)

    backtester = Backtester(config)
    results = asyncio.run(backtester.run())

    # (Result printing code remains the same)
    print("\n--- Backtest Summary ---")
    # ... (rest of the result printing)