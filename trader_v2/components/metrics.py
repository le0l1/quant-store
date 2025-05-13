from components.base import BaseComponent
from components.portfolio import BaseBroker
from core.event_bus import EventBus
import logging
import pandas as pd
import quantstats as qs  # Import quantstats for performance metrics
from core.events import MarketEvent

logger = logging.getLogger(__name__)


class Metrics(BaseComponent):
    def __init__(self, event_bus: EventBus, portfolio: BaseBroker):
        super().__init__(event_bus)
        self.portfolio = portfolio
        self._net_data = list()
        self.holds = pd.DataFrame(columns=['timestamp', 'symbol', 'quantity', 'price'])

    def _setup_event_handlers(self):
        self.event_bus.subscribe(MarketEvent, self.on_market_event)
        # pass # No longer needed

    async def on_market_event(self, market_event: MarketEvent):
        self._net_data.append([
            market_event.timestamp,
            self.portfolio.get_total_portfolio_value()
        ])
        positions_df = self.portfolio.get_positions()
        if not positions_df.empty:
            positions_df['timestamp'] = market_event.timestamp
            self.holds = pd.concat([self.holds, positions_df], ignore_index=True)

    def get_metrics(self):
        data = self._net_data
        if not data:
            logger.warning("No data available to calculate metrics.")
            return None # Or an empty dict, or raise an error

        df = pd.DataFrame(data, columns=['timestamp', 'netvalue'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        returns = df['netvalue'].pct_change().dropna()

        if returns.empty:
            logger.warning("Not enough data to calculate returns for metrics.")
            return {
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "max_drawdown": 0.0,
                "cumulative_return": 0.0,
                "win_ratio": 0.0,
                "volatility": 0.0,
            }

        qs.extend_pandas()

        metrics_data = {
            "sharpe_ratio": qs.stats.sharpe(returns),
            "sortino_ratio": qs.stats.sortino(returns),
            "max_drawdown": qs.stats.max_drawdown(returns),
            "cumulative_return": qs.stats.cagr(returns), # Note: CAGR might need adjustment based on period
            "win_ratio": qs.stats.win_rate(returns),
            "volatility": qs.stats.volatility(returns)
        }
        return metrics_data


    def display_metrics(self):
        metrics_data = self.get_metrics() # Use the get_metrics method
        if metrics_data is None: # get_metrics handles the no data warning
            return

        logger.info("=== 策略性能指标 ===")
        logger.info(f"夏普比率 (Sharpe Ratio): {metrics_data['sharpe_ratio']:.4f}")
        logger.info(f"索提诺比率 (Sortino Ratio): {metrics_data['sortino_ratio']:.4f}")
        logger.info(f"最大回撤 (Max Drawdown): {metrics_data['max_drawdown']:.4f}")
        logger.info(f"年化累计收益率 (CAGR): {metrics_data['cumulative_return']:.4f}")
        logger.info(f"胜率 (Win Ratio): {metrics_data['win_ratio']:.4f}")
        logger.info(f"年化波动率 (Volatility): {metrics_data['volatility']:.4f}")

    def plot_net_value_curve(self):
        data = self._net_data
        df = pd.DataFrame(data, columns=['timestamp', 'netvalue'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        self.holds.to_csv('holds.csv')
        return df['netvalue'].to_csv('returns.csv')
    
