from components.base import BaseComponent
from components.portfolio import BasePortfolio
from core.event_bus import EventBus
import logging
import pandas as pd
import quantstats as qs  # Import quantstats for performance metrics
from core.events import MarketEvent

logger = logging.getLogger(__name__)


class Metrics(BaseComponent):
    def __init__(self, event_bus: EventBus, portfolio: BasePortfolio):
        super().__init__(event_bus)
        self.portfolio = portfolio
        self._net_data = list()

    def _setup_event_handlers(self):
        self.event_bus.subscribe(MarketEvent, self.on_market_event)
        pass
    
    async def on_market_event(self, market_event: MarketEvent):
        self._net_data.append([
            market_event.timestamp,
            self.portfolio.get_total_portfolio_value()
        ])

    def get_metrics(self):
        # Calculate metrics based on the data
        pass

    def display_metrics(self):
        data = self._net_data
        if not data:
            logger.warning("No data available to calculate metrics.")
            return
        df = pd.DataFrame(data, columns=['timestamp', 'netvalue'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # 转换为 datetime 格式
        df.set_index('timestamp', inplace=True)  # 设置时间戳为索引

        returns = df['netvalue'].pct_change().dropna()
        qs.extend_pandas()  # 扩展 pandas 功能

        sharpe_ratio = qs.stats.sharpe(returns)  # 夏普比率
        sortino_ratio = qs.stats.sortino(returns)  # 索提诺比率
        max_drawdown = qs.stats.max_drawdown(returns)  # 最大回撤
        cumulative_return = qs.stats.cagr(returns)  # 年化累计收益率
        win_ratio = qs.stats.win_rate(returns)  # 胜率
        volatility = qs.stats.volatility(returns)  # 年化波动率

        logger.info("=== 策略性能指标 ===")
        logger.info(f"夏普比率 (Sharpe Ratio): {sharpe_ratio:.4f}")
        logger.info(f"索提诺比率 (Sortino Ratio): {sortino_ratio:.4f}")
        logger.info(f"最大回撤 (Max Drawdown): {max_drawdown:.4f}")
        logger.info(f"年化累计收益率 (CAGR): {cumulative_return:.4f}")
        logger.info(f"胜率 (Win Ratio): {win_ratio:.4f}")
        logger.info(f"年化波动率 (Volatility): {volatility:.4f}")
