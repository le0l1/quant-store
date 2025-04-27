# backtester.py
import logging
import time
from datetime import datetime
from typing import Dict, Any, Type, Optional, List, Tuple
import importlib # To load classes dynamically

from event_engine import EventEngine
from event import SystemEvent
# Import interfaces and concrete classes we need to instantiate
from data_source import IDataSource
from data_handler import IDataHandler, BasicDataHandler
from strategy import IStrategy, IPortfolioManager # For type hint
from execution import IExecutionHandler, BacktestExecutionHandler
from portfolio import IPortfolioManager, BasicPortfolioManager, Position # Need Position for results typing


logger = logging.getLogger(__name__)

class Backtester:
    """
    封装了整个回测流程的类。
    用户提供配置、策略类和参数即可运行回测。
    """
    def __init__(self, config: Dict[str, Any]):
        """
        初始化 Backtester。

        :param config: 包含所有回测设置的字典。预期键值包括:
                       'initial_capital', 'start_date', 'end_date', 'symbols',
                       'data_source_config': {'class': Type[IDataSource], 'params': Dict},
                       'strategy_config': {'class': Type[IStrategy], 'params': Dict},
                       'execution_config': {'commission_rate': float, 'slippage_per_trade': float},
                       'portfolio_config': {} (可能为空或包含特定设置),
                       'data_handler_config': {} (可能为空或包含特定设置, e.g., 'max_bars'),
                       'performance_analyzer_config': {'class': Type[IPerformanceAnalyzer], 'params': {}} (可选)
        """
        self.config = config
        self._validate_config() # Ensure essential keys exist

        self._engine: Optional[EventEngine] = None
        self._data_source: Optional[IDataSource] = None
        self._data_handler: Optional[IDataHandler] = None
        self._portfolio_manager: Optional[IPortfolioManager] = None
        self._execution_handler: Optional[IExecutionHandler] = None
        self._strategy_instance: Optional[IStrategy] = None

        self._start_time = 0.0
        self._results: Dict[str, Any] = {}

        self._set_defaults()

        logging.basicConfig(level=config.get('log_level', 'INFO'), # Allow config override
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info("Backtester initialized with configuration.")


    def _validate_config(self):
        """检查核心配置键。"""
        required_keys = ['initial_capital', 'start_date', 'end_date', 'symbols',
                         'data_source_config', 'strategy_config']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"配置中缺少核心键: '{key}'")
        if 'class' not in self.config['data_source_config']:
             raise ValueError("data_source_config 缺少 'class'")
        if 'class' not in self.config['strategy_config']:
             raise ValueError("strategy_config 缺少 'class'")
        logger.debug("Core configuration validated.")

    def _set_defaults(self):
        """为可选组件设置默认配置。"""
        # Execution Handler Defaults
        if 'execution_config' not in self.config:
             self.config['execution_config'] = {}
        self.config['execution_config'].setdefault('class', BacktestExecutionHandler)
        self.config['execution_config'].setdefault('commission_rate', 0.0)
        self.config['execution_config'].setdefault('slippage_per_trade', 0.0)

        # Portfolio Manager Defaults
        if 'portfolio_config' not in self.config:
             self.config['portfolio_config'] = {}
        self.config['portfolio_config'].setdefault('class', BasicPortfolioManager)
        self.config['portfolio_config'].setdefault('params', {})

        # Data Handler Defaults
        if 'data_handler_config' not in self.config:
             self.config['data_handler_config'] = {}
        self.config['data_handler_config'].setdefault('class', BasicDataHandler)
        self.config['data_handler_config'].setdefault('params', {})


    def _setup_components(self):
        """根据配置（包括默认值）实例化所有框架组件。"""
        logger.info("Setting up backtest components...")
        self._engine = EventEngine()
        initial_capital = self.config['initial_capital']
        symbols = self.config['symbols']

        # --- 使用配置（包含默认值）实例化 ---
        # Portfolio Manager
        portfolio_cls = self.config['portfolio_config']['class']
        portfolio_params = self.config['portfolio_config']['params']
        self._portfolio_manager = portfolio_cls(self._engine, initial_capital, **portfolio_params)

        # Data Handler
        dh_cls = self.config['data_handler_config']['class']
        dh_params = self.config['data_handler_config']['params']
        self._data_handler = dh_cls(self._engine, symbols, **dh_params)

        # Strategy (Requires dependencies)
        strategy_config = self.config['strategy_config']
        StrategyCls = strategy_config['class']
        strategy_params = strategy_config.get('params', {})
        self._strategy_instance = StrategyCls(
            strategy_id=strategy_config.get('id', StrategyCls.__name__),
            symbols=symbols,
            event_engine=self._engine,
            data_handler=self._data_handler,
            portfolio_manager=self._portfolio_manager,
            **strategy_params
        )
        self._strategy_instance.register_event_listeners()

        # Execution Handler
        exec_config = self.config['execution_config']
        exec_cls = exec_config['class']
        exec_params_combined = { # Combine specific and general params
            'commission_rate': exec_config.get('commission_rate'),
            'slippage_per_trade': exec_config.get('slippage_per_trade'),
            **(exec_config.get('params', {}))
        }
        # Filter None values if constructor doesn't accept them explicitly
        exec_params_filtered = {k: v for k, v in exec_params_combined.items() if v is not None}
        self._execution_handler = exec_cls(self._engine, **exec_params_filtered)


        # Data Source
        ds_config = self.config['data_source_config']
        DsCls = ds_config['class']
        ds_params = ds_config.get('params', {})
        self._data_source = DsCls(
            event_engine=self._engine,
            symbols=symbols,
            start_date=self.config['start_date'],
            end_date=self.config['end_date'],
            **ds_params
        )

        logger.info("All components set up successfully using resolved configurations.")

    def run(self) -> Dict[str, Any]:
        """执行完整的的回测流程。"""
        self._start_time = time.time()
        logger.info("========== Running Backtest ==========")

        # 1. Setup
        if not self._engine: # Only setup if not already done
             self._setup_components()

        # Ensure components are set
        if not all([self._engine, self._data_source, self._portfolio_manager, self._strategy_instance]):
             raise RuntimeError("Backtester components were not set up correctly.")

        # 2. Start Engine
        logger.info("Starting event engine...")
        self._engine.start()

        # 3. Activate Strategy (calls on_start)
        logger.info("Activating strategy...")
        self._strategy_instance.activate()

        # 4. Drive Data
        logger.info("Starting data stream...")
        self._data_source.start()
        data_stream = self._data_source.get_stream()

        # Emit start event
        self._engine.put(SystemEvent(timestamp=self.config['start_date'] or datetime.now(), message="BACKTEST_START"))

        event_count = 0
        last_event_time = self.config['start_date']
        for market_event in data_stream:
            self._engine.put(market_event)
            event_count += 1
            last_event_time = market_event.timestamp
            if event_count % 1000 == 0:
                logger.info(f"   Processed {event_count} market events, last timestamp: {last_event_time}")

        logger.info(f"Finished putting {event_count} market events.")
        self._data_source.stop()

        # 5. Wait for Queue
        logger.info("Waiting for event queue to empty...")
        while self._engine.qsize() > 0:
            time.sleep(0.2)
        logger.info("Event queue empty.")

        # Emit end event
        self._engine.put(SystemEvent(timestamp=last_event_time or datetime.now(), message="BACKTEST_END"))
        time.sleep(0.5) # Allow final events to process

        # 6. Deactivate Strategy (calls on_stop)
        logger.info("Deactivating strategy...")
        self._strategy_instance.deactivate()

        # 7. Stop Engine
        logger.info("Stopping event engine...")
        self._engine.stop(wait=True)

        # 8. Collect Results
        self._results = self._collect_results()

        end_time = time.time()
        self._results['runtime_seconds'] = round(end_time - self._start_time, 2)
        logger.info(f"Backtest finished in {self._results['runtime_seconds']:.2f} seconds.")
        logger.info("====================================")

        return self._results

    def _collect_results(self) -> Dict[str, Any]:
        """收集来自 Portfolio Manager 和 Performance Analyzer 的结果。"""
        if not self._portfolio_manager:
            return {"error": "Portfolio Manager not available."}

        portfolio_summary = {
            "initial_capital": self.config['initial_capital'],
            "final_equity": self._portfolio_manager.get_current_equity(),
            "final_cash": self._portfolio_manager.get_cash(),
            "final_holdings_value": self._portfolio_manager.get_current_holdings_value(),
        }
        portfolio_summary["total_return_pct"] = round(
            (portfolio_summary["final_equity"] - portfolio_summary["initial_capital"])
            / portfolio_summary["initial_capital"] * 100, 4
        ) if portfolio_summary["initial_capital"] else 0.0

        equity_curve = self._portfolio_manager.get_equity_curve()
        all_positions = self._portfolio_manager.get_all_positions()

        performance_stats = {}

        return {
            "summary": portfolio_summary,
            "equity_curve": equity_curve, # List of (datetime, float) tuples
            "final_positions": {sym: pos.__dict__ for sym, pos in all_positions.items()}, # Convert Position objects
            "performance_metrics": performance_stats
        }

    def get_results(self) -> Dict[str, Any]:
        """返回最后一次运行的回测结果。"""
        return self._results