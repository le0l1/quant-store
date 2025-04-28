import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
import asyncio
import traceback

from trader.base.event_engine import EventEngine
from trader.base.event import Event, SystemEvent, FillEvent, PortfolioUpdateEvent
from trader.base import DataFeed, Strategy, PortfolioManager, Execution
from trader.backtester.execution import BacktestExecution
from trader.backtester.portfolio import BacktestPortfolioManager
from trader.base.data_handler import DataHandler


def _validate_config_func(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Validates the simplified configuration dictionary."""
    required_keys = ['initial_capital', 'symbols', 'data_source_config', 'strategy_config']
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    
    if not isinstance(config['initial_capital'], (int, float)) or config['initial_capital'] <= 0:
        raise ValueError("initial_capital must be a positive number")
    
    if not isinstance(config['symbols'], list) or not config['symbols']:
        raise ValueError("symbols must be a non-empty list")
    
    for cfg in ['data_source_config', 'strategy_config']:
        if 'class' not in config[cfg]:
            raise ValueError(f"{cfg}.class must be specified")
        if 'params' in config[cfg] and not isinstance(config[cfg]['params'], dict):
            raise ValueError(f"{cfg}.params must be a dictionary")

def _set_defaults_func(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Sets default values for optional configuration parameters."""
    config.setdefault('log_level', 'INFO')
    config.setdefault('start_date', None)
    config.setdefault('end_date', None)
    config['strategy_config'].setdefault('id', config['strategy_config']['class'].split('.')[-1])
    config['strategy_config'].setdefault('params', {})
    config['data_source_config'].setdefault('params', {})
    logger.debug(f"Configuration after defaults: {config}")

def _resolve_class(class_path: str) -> type:
    """Resolves a class from a string path (e.g., 'module.submodule.ClassName')."""
    try:
        module_name, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Failed to resolve class {class_path}: {e}")

def _collect_results_func(
    portfolio_manager: Optional[PortfolioManager],
    logger: logging.Logger
) -> Dict[str, Any]:
    """Collects results primarily from the portfolio manager."""
    logger.debug("Collecting results...")
    results = {"status": "completed"}

    if portfolio_manager:
        try:
            initial_capital = getattr(portfolio_manager, 'initial_capital', 'N/A')
            final_cash = portfolio_manager.get_cash()
            final_holdings = portfolio_manager.get_current_holdings_value()
            final_total_value = final_cash + final_holdings if isinstance(final_cash, (int, float)) and isinstance(final_holdings, (int, float)) else 'N/A'

            portfolio_summary = {
                "initial_capital": initial_capital,
                "final_cash": final_cash,
                "final_holdings_value": final_holdings,
                "final_total_value": final_total_value,
                "total_return_pct": ((final_total_value - initial_capital) / initial_capital) * 100
                                    if isinstance(initial_capital, (int, float)) and initial_capital != 0 and isinstance(final_total_value, (int, float))
                                    else 0.0 if initial_capital == 0 else 'N/A',
            }
            results['portfolio_summary'] = portfolio_summary
            results['equity_curve'] = portfolio_manager.get_equity_curve() if hasattr(portfolio_manager, 'get_equity_curve') else []
            final_positions_obj = portfolio_manager.get_all_positions()
            results['final_positions'] = {
                sym: pos.to_dict() if hasattr(pos, 'to_dict') else vars(pos)
                for sym, pos in final_positions_obj.items()
            } if final_positions_obj else {}
            logger.info(f"Collected portfolio results: {portfolio_summary}")

        except Exception as e:
            logger.error(f"Error collecting portfolio results: {e}", exc_info=True)
            results["portfolio_summary"] = {"error": f"Error collecting portfolio results: {e}"}
            results['final_positions'] = {"error": "Portfolio data unavailable"}
            results['equity_curve'] = []
    else:
        logger.warning("Portfolio Manager not available for results collection.")
        results["portfolio_summary"] = {"error": "Portfolio Manager not available."}
        results['final_positions'] = {}
        results['equity_curve'] = []

    results['performance_metrics'] = {}
    return results

def _print_summary(results: Dict[str, Any], logger: logging.Logger):
    """Prints a summary of the backtest results."""
    print("\n--- Backtest Summary ---")
    summary = results.get('portfolio_summary', {})
    if isinstance(summary, dict) and "error" not in summary:
        init_cap = summary.get('initial_capital', 'N/A')
        final_val = summary.get('final_total_value', 'N/A')
        ret_pct = summary.get('total_return_pct', 'N/A')
        print(f"  Initial Capital: {init_cap:,.2f}" if isinstance(init_cap, (int, float)) else f"  Initial Capital: {init_cap}")
        print(f"  Final Cash: {summary.get('final_cash', 'N/A'):,.2f}" if isinstance(summary.get('final_cash'), (int, float)) else f"  Final Cash: {summary.get('final_cash', 'N/A')}")
        print(f"  Final Holdings Value: {summary.get('final_holdings_value', 'N/A'):,.2f}" if isinstance(summary.get('final_holdings_value'), (int, float)) else f"  Final Holdings Value: {summary.get('final_holdings_value', 'N/A')}")
        print(f"  Final Total Value: {final_val:,.2f}" if isinstance(final_val, (int, float)) else f"  Final Total Value: {final_val}")
        print(f"  Total Return: {ret_pct:.2f}%" if isinstance(ret_pct, (int, float)) else f"  Total Return: {ret_pct}")
    else:
        print(f"  Error collecting portfolio summary: {summary.get('error', 'Unknown error')}")

    print("\n--- Final Positions ---")
    final_positions = results.get('final_positions', {})
    if isinstance(final_positions, dict) and "error" not in final_positions:
        if final_positions:
            for symbol, pos_data in final_positions.items():
                qty = pos_data.get('quantity', 'N/A')
                avg_price = pos_data.get('average_price', 'N/A')
                market_val = pos_data.get('market_value', 'N/A')
                print(f"  {symbol}: Qty={qty}, AvgPrice={avg_price:.2f}, MarketValue={market_val:.2f}"
                      if isinstance(avg_price, (int, float)) and isinstance(market_val, (int, float))
                      else f"  {symbol}: Qty={qty}, AvgPrice={avg_price}, MarketValue={market_val}")
        else:
            print("  No positions held at the end.")
    else:
        print(f"  Error collecting final positions: {final_positions.get('error', 'Unknown error')}")

    runtime = results.get('runtime_seconds', 'N/A')
    print(f"\nRuntime: {runtime:.2f} seconds" if isinstance(runtime, float) else f"\nRuntime: {runtime}")
    print("------------------------")

async def run_backtest(config: Dict[str, Any]) -> Dict[str, Any]:
    """Runs a simplified backtest with fixed portfolio, execution, and data handlers."""
    # Setup logging
    log_level = config.get('log_level', 'INFO').upper()
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("run_backtest")
    logger.info("========== Starting Simplified Async Backtest ==========")

    start_time = time.time()
    results: Dict[str, Any] = {"status": "failed_setup"}
    event_engine = None

    try:
        # Validate and set defaults
        _validate_config_func(config, logger)
        _set_defaults_func(config, logger)

        engine = EventEngine()
        initial_capital = config['initial_capital']
        symbols = config['symbols']
        portfolio_manager = BacktestPortfolioManager(engine, initial_capital)
        data_handler = DataHandler(engine, symbols)
        execution_handler = BacktestExecution(engine, commission_rate=0.001, slippage_per_trade=0.0)

        # Instantiate fixed components

        # Instantiate Strategy
        strategy_cls = _resolve_class(config['strategy_config']['class'])
        strategy_params = config['strategy_config'].get('params', {})
        strategy_id = config['strategy_config'].get('id', strategy_cls.__name__)
        strategy_instance = strategy_cls(
            strategy_id=strategy_id,
            symbols=symbols,
            event_engine=engine,
            data_handler=data_handler,
            portfolio_manager=portfolio_manager,
            **strategy_params
        )
        if hasattr(strategy_instance, 'register_event_listeners'):
            strategy_instance.register_event_listeners()
        logger.info(f"Instantiated Strategy: {strategy_cls.__name__} (ID: {strategy_id})")

        # Instantiate Data Source
        ds_cls = _resolve_class(config['data_source_config']['class'])
        ds_params = config['data_source_config'].get('params', {})
        data_source = ds_cls(
            event_engine=engine, symbols=symbols,
            start_date=config.get('start_date'), end_date=config.get('end_date'),
            **ds_params
        )
        logger.info(f"Instantiated Data Source: {ds_cls.__name__}")

        # Ensure all components are instantiated
        if not all([engine, data_source, portfolio_manager, strategy_instance, execution_handler, data_handler]):
            raise RuntimeError("One or more essential backtester components failed to initialize.")
        logger.info("All required components instantiated successfully.")

        # Run the backtest
        logger.info("Starting event engine...")
        await engine.start()
        strategy_instance.activate()
        await data_source.start()
        strategy_instance.deactivate()
        await engine.stop()

        # Collect results
        results = _collect_results_func(portfolio_manager, logger)
        results["status"] = "completed"
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        results["status"] = "error_unexpected"
        results["error_message"] = f"Unexpected error: {type(e).__name__} - {e}"
    finally:
        if engine and engine.is_running():
            logger.warning("Engine still running, attempting forced stop.")
            try:
                await engine.stop()
                logger.info("Engine stopped in finally block.")
            except Exception as stop_err:
                logger.error(f"Error stopping engine: {stop_err}", exc_info=True)

        end_time = time.time()
        results['runtime_seconds'] = round(end_time - start_time, 2)
        logger.info(f"Backtest finished. Status: {results.get('status', 'unknown')}. Runtime: {results['runtime_seconds']:.2f} seconds.")
        _print_summary(results, logger)
        return results