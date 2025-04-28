# backtester.py (Modified for AsyncEventEngine)

import logging
import time
from datetime import datetime
from typing import Dict, Any, Type, Optional, List, Tuple
import importlib
from abc import ABC, abstractmethod
import asyncio 

from trader.base.event_engine import EventEngine # 修正后的正确导入路径
from trader.base.event import Event, SystemEvent, FillEvent, PortfolioUpdateEvent
from trader.base import IDataSource,IStrategy,IPortfolioManager,IExecutionHandler
from trader.backtest.execution import BacktestExecutionHandler
from trader.backtest.portfolio import BasicPortfolioManager
from trader.base.data_handler import BasicDataHandler, IDataHandler

logger = logging.getLogger(__name__)

# --- Performance Analyzer Interfacfrom trader.base.event import Evene & Basic Implementation ---
# (This part remains unchanged conceptually, but handlers might need async)
class IPerformanceAnalyzer(ABC):
    """性能分析器接口。"""
    def __init__(self, event_engine: EventEngine): # <--- Type hint engine
        self.event_engine = event_engine
        self._register_listeners()

    def _register_listeners(self):
        """注册监听事件。"""
        # Registering sync or async handlers works the same way
        self.event_engine.register(FillEvent.event_type, self.on_event)
        self.event_engine.register(PortfolioUpdateEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 已注册监听 {FillEvent.event_type} 和 {PortfolioUpdateEvent.event_type}。")

    @abstractmethod
    # Handler itself might become async def if needed
    def on_event(self, event: Event):
        """处理事件，记录数据。"""
        pass

    @abstractmethod
    def get_results(self) -> Dict[str, Any]:
        """计算并返回性能指标。"""
        pass

class BasicPerformanceAnalyzer(IPerformanceAnalyzer):
    """基础性能分析器。"""
    def __init__(self, event_engine: EventEngine): # <--- Type hint engine
        super().__init__(event_engine)
        self.fill_count = 0
        self.last_equity = 0.0
        logger.info("BasicPerformanceAnalyzer 初始化。")

    # This can remain sync if logic is simple, or become async def
    def on_event(self, event: Event):
        if event.event_type == FillEvent.event_type:
            self.fill_count += 1
            logger.debug(f"Performance Analyzer 记录 Fill #{self.fill_count}")
        elif event.event_type == PortfolioUpdateEvent.event_type:
             if isinstance(event, PortfolioUpdateEvent):
                 self.last_equity = event.total_value

    def get_results(self) -> Dict[str, Any]:
        logger.info("生成基础性能结果...")
        results = {"total_trades": self.fill_count}
        logger.info(f"性能指标: {results}")
        return results

# --- Backtester Class ---
class Backtester:
    """
    封装了整个回测流程的类 - 使用 AsyncEventEngine。
    """
    def __init__(self, config: Dict[str, Any]):
        """
        初始化 Backtester。
        """
        self.config = config
        self.mode = 'backtest'
        self._validate_config()
        self._set_defaults()

        # --- Type hint uses the async version ---
        self._engine: Optional[EventEngine] = None
        # --- Other components remain the same type hints ---
        self._data_source: Optional[IDataSource] = None
        self._data_handler: Optional[IDataHandler] = None
        self._portfolio_manager: Optional[IPortfolioManager] = None
        self._execution_handler: Optional[IExecutionHandler] = None
        self._strategy_instance: Optional[IStrategy] = None
        self._performance_analyzer: Optional[IPerformanceAnalyzer] = None

        self._start_time = 0.0
        self._results: Dict[str, Any] = {}

        logging.basicConfig(level=self.config.get('log_level', 'INFO'),
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info(f"Async Backtester initialized (Mode: {self.mode}) with configuration.")

    def _validate_config(self):
        """检查核心配置键。"""
        # (Validation logic remains the same)
        required_keys = ['initial_capital', 'symbols', 'data_source_config', 'strategy_config']
        for key in required_keys:
            if key not in self.config: raise ValueError(f"配置中缺少核心键: '{key}'")
        if 'class' not in self.config['data_source_config']: raise ValueError("data_source_config 缺少 'class'")
        if 'class' not in self.config['strategy_config']: raise ValueError("strategy_config 缺少 'class'")
        logger.debug("Core configuration validated.")

    def _set_defaults(self):
        """为可选组件设置默认配置。"""
        # (Defaults logic remains the same)
        exec_conf = self.config.setdefault('execution_config', {})
        exec_conf.setdefault('class', BacktestExecutionHandler)
        exec_conf.setdefault('commission_rate', 0.0); exec_conf.setdefault('slippage_per_trade', 0.0)
        port_conf = self.config.setdefault('portfolio_config', {}); port_conf.setdefault('class', BasicPortfolioManager); port_conf.setdefault('params', {})
        dh_conf = self.config.setdefault('data_handler_config', {}); dh_conf.setdefault('class', BasicDataHandler); dh_conf.setdefault('params', {})
        pa_conf = self.config.setdefault('performance_analyzer_config', {}); pa_conf.setdefault('class', BasicPerformanceAnalyzer); pa_conf.setdefault('params', {})
        logger.debug("Default configurations applied.")

    def _setup_components(self):
        """根据配置实例化所有框架组件。"""
        logger.info("Setting up backtest components...")
        # --- Instantiate Async Event Engine ---
        self._engine = EventEngine(mode=self.mode) # <--- Use AsyncEventEngine

        initial_capital = self.config['initial_capital']
        symbols = self.config['symbols']

        # --- Instantiate other components, passing the AsyncEventEngine ---
        # Portfolio Manager
        portfolio_cls = self.config['portfolio_config']['class']
        portfolio_params = self.config['portfolio_config']['params']
        # Assuming PM init takes engine as first arg
        self._portfolio_manager = portfolio_cls(self._engine, initial_capital, **portfolio_params)

        # Data Handler
        dh_cls = self.config['data_handler_config']['class']
        dh_params = self.config['data_handler_config']['params']
        # Assuming DH init takes engine, symbols
        self._data_handler = dh_cls(self._engine, symbols, **dh_params)

        # Strategy
        strategy_config = self.config['strategy_config']
        StrategyCls = strategy_config['class']
        strategy_params = strategy_config.get('params', {})
        # Assuming Strategy init takes id, symbols, engine, data_handler, portfolio_manager
        self._strategy_instance = StrategyCls(
            strategy_id=strategy_config.get('id', StrategyCls.__name__), symbols=symbols,
            event_engine=self._engine, data_handler=self._data_handler,
            portfolio_manager=self._portfolio_manager, **strategy_params
        )
        self._strategy_instance.register_event_listeners() # Assuming this method just calls engine.register

        # Execution Handler
        exec_config = self.config['execution_config']
        exec_cls = exec_config['class']
        exec_params_combined = {
            'commission_rate': exec_config.get('commission_rate'),
            'slippage_per_trade': exec_config.get('slippage_per_trade'),
            **(exec_config.get('params', {}))
        }
        exec_params_filtered = {k: v for k, v in exec_params_combined.items() if v is not None}
         # Assuming Exec init takes engine as first arg
        self._execution_handler = exec_cls(self._engine, **exec_params_filtered)

        # Data Source
        ds_config = self.config['data_source_config']
        DsCls = ds_config['class']
        ds_specific_params = ds_config.get('params', {})
         # Assuming DS init takes engine, symbols, start_date, end_date + specifics
        self._data_source = DsCls(
            event_engine=self._engine, symbols=symbols,
            start_date=self.config.get('start_date'), end_date=self.config.get('end_date'),
            **ds_specific_params
        )

        # Performance Analyzer
        pa_config = self.config['performance_analyzer_config']
        pa_cls = pa_config['class']
        pa_params = pa_config.get('params', {})
         # Assuming PA init takes engine as first arg
        self._performance_analyzer = pa_cls(self._engine, **pa_params)

        logger.info("All components set up successfully.")

    # --- Make run method asynchronous ---
    async def run(self) -> Dict[str, Any]:
        self._start_time = time.time()
        logger.info(f"========== Running Async Backtest (Mode: {self.mode}) ==========")

        # 1. Setup
        if not self._engine: self._setup_components()
        if not all([self._engine, self._data_source, self._portfolio_manager,
                    self._strategy_instance, self._execution_handler, self._data_handler,
                    self._performance_analyzer]):
             raise RuntimeError("Backtester components were not set up correctly.")

        # 2. Start Engine (await async start)
        await self._engine.start() # <--- Use await
        # 3. Activate Strategy
        self._strategy_instance.activate() # Assuming activate itself is sync
        # 4. Drive Data and Run Async Sync Event Loop
        logger.info("Starting data stream and processing events (async sync cycle)...")
        await self._data_source.start()
        
        await self._data_source.stop()
        # 6. Deactivate Strategy
        self._strategy_instance.deactivate() # Assuming deactivate is sync
        # 7. Stop Engine (await async stop)
        await self._engine.stop() # <--- Use await

        # 8. Collect Results (assuming sync collection)
        logger.info("Collecting backtest results...")
        self._results = self._collect_results() # Assuming sync

        end_time = time.time()
        self._results['runtime_seconds'] = round(end_time - self._start_time, 2)
        logger.info(f"Backtest finished in {self._results['runtime_seconds']:.2f} seconds.")
        logger.info("====================================")

        self._print_summary(self._results)
        return self._results

    def _collect_results(self) -> Dict[str, Any]:
        """Collects results from portfolio manager and performance analyzer."""
        logger.debug("Collecting results...")
        # Initialize results dict with default status
        results = {"status": "completed"}

        # --- Portfolio Results ---
        if self._portfolio_manager:
            try:
                # Retrieve values needed for the summary
                initial_capital = getattr(self._portfolio_manager, 'initial_capital', 'N/A') # Safer access
                final_cash = self._portfolio_manager.get_cash()
                final_holdings = self._portfolio_manager.get_current_holdings_value()
                final_total_value = final_cash + final_holdings

                # --- FIX: Initialize as a dictionary and calculate return ---
                portfolio_summary = {
                    "initial_capital": initial_capital,
                    "final_cash": final_cash,
                    "final_holdings_value": final_holdings,
                    "final_total_value": final_total_value,
                    # Calculate return percentage correctly, handle division by zero
                    "total_return_pct": ((final_total_value - initial_capital) / initial_capital) * 100
                                        if isinstance(initial_capital, (int, float)) and initial_capital != 0
                                        else 0.0,
                }
                results['portfolio_summary'] = portfolio_summary
                # --- End Fix ---

                # Get other portfolio details
                results['equity_curve'] = self._portfolio_manager.get_equity_curve()
                final_positions_obj = self._portfolio_manager.get_all_positions()
                # Convert Position objects to dicts (check if method exists)
                results['final_positions'] = {
                    sym: pos.to_dict() if hasattr(pos, 'to_dict') else vars(pos)
                    for sym, pos in final_positions_obj.items()
                }
                logger.info(f"Collected portfolio results: {results['portfolio_summary']}")

            except AttributeError as e:
                logger.error(f"Portfolio Manager missing expected attribute/method: {e}", exc_info=True)
                results["portfolio_summary"] = {"error": f"Portfolio Manager missing method/attr: {e}"}
                results['final_positions'] = {"error": "Portfolio data unavailable"}
                results['equity_curve'] = []
            except Exception as e:
                logger.error(f"Error collecting portfolio results: {e}", exc_info=True)
                results["portfolio_summary"] = {"error": f"Error collecting portfolio results: {e}"}
                results['final_positions'] = {"error": "Portfolio data unavailable"}
                results['equity_curve'] = []
        else:
            logger.warning("Portfolio Manager not available for results collection.")
            # Provide default empty structures
            results["portfolio_summary"] = {"error": "Portfolio Manager not available."}
            results['final_positions'] = {}
            results['equity_curve'] = []

        # --- Performance Analyzer Results ---
        if self._performance_analyzer:
            try:
                results['performance_metrics'] = self._performance_analyzer.get_results()
                logger.debug(f"Collected performance metrics: {results['performance_metrics']}")
            except Exception as e:
                 logger.error(f"Error collecting performance results: {e}", exc_info=True)
                 results['performance_metrics'] = {"error": f"Error collecting performance results: {e}"}
        else:
            logger.warning("Performance Analyzer not available for results collection.")
            results['performance_metrics'] = {"error": "Performance Analyzer not available."}

        # --- Ensure overall status reflects collection errors ---
        if "error" in results["portfolio_summary"] or "error" in results['performance_metrics']:
             results["status"] = "error_in_collection"

        return results

    def get_results(self) -> Dict[str, Any]:
        """返回结果 (Sync)。"""
        return self._results

    def _print_summary(self, results: Dict[str, Any]):
        """打印结果 (Sync)。"""
        # (Remains the same)
        print("\n--- Backtest Summary ---"); summary = ...; # print loop
        print("\n--- Performance Metrics ---"); perf_metrics = ...; # print loop or message
        print("\n--- Final Positions ---"); final_positions = ...; # print loop or message
        print(f"\nRuntime: {results.get('runtime_seconds', 'N/A')} seconds")
        print("------------------------")