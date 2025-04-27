# portfolio.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Tuple
import pandas as pd # For equity curve potentially
import logging

from event import Event, FillEvent, MarketEvent, PortfolioUpdateEvent
from event_engine import EventEngine

logger = logging.getLogger(__name__)

@dataclass
class Position:
    """Represents the holding of a specific asset."""
    symbol: str
    quantity: float = 0.0           # Current holding quantity (can be negative for shorts later)
    average_price: float = 0.0      # Average entry price of the current holding
    last_update_time: Optional[datetime] = None # Time of the last fill affecting this position
    market_price: float = 0.0       # Last known market price for Mark-to-Market
    market_value: float = 0.0       # Current market value (quantity * market_price)
    unrealized_pnl: float = 0.0     # Profit/Loss if position were closed now

# --- Portfolio Manager Interface ---
class IPortfolioManager(ABC):
    """
    投资组合管理器接口 (抽象基类)。
    """
    def __init__(self, event_engine: EventEngine, initial_capital: float):
        self.event_engine = event_engine
        self.initial_capital = initial_capital
        # Register necessary event handlers
        self.register_event_handler(event_engine)
        logger.info(f"{self.__class__.__name__} 初始化。")

    def register_event_handler(self, engine: EventEngine):
        """注册需要监听的事件。"""
        engine.register(FillEvent.event_type, self.on_event)
        engine.register(MarketEvent.event_type, self.on_event)
        logger.info(f"{self.__class__.__name__} 已注册监听 {FillEvent.event_type} 和 {MarketEvent.event_type}。")

    @abstractmethod
    def on_event(self, event: Event):
        """处理事件的核心入口。"""
        pass

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定标的的持仓信息。"""
        pass

    @abstractmethod
    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有当前持仓。"""
        pass

    @abstractmethod
    def get_cash(self) -> float:
        """获取当前可用现金。"""
        pass

    @abstractmethod
    def get_current_holdings_value(self) -> float:
        """获取当前所有持仓的总市值。"""
        pass

    @abstractmethod
    def get_current_equity(self) -> float:
        """获取当前总权益 (现金 + 持仓市值)。"""
        pass

    @abstractmethod
    def get_equity_curve(self) -> List[Tuple[datetime, float]]:
        """获取权益曲线的时间序列。"""
        pass

# --- Basic Portfolio Manager Implementation ---
class BasicPortfolioManager(IPortfolioManager):
    """
    基础的投资组合管理器实现。
    处理 FillEvent 更新持仓和现金。
    处理 MarketEvent 更新市值和权益。
    """
    def __init__(self, event_engine: EventEngine, initial_capital: float):
        super().__init__(event_engine, initial_capital)

        self._current_cash: float = initial_capital
        # key: symbol, value: Position object
        self._positions: Dict[str, Position] = {}
        # key: symbol, value: last seen market price
        self._latest_market_prices: Dict[str, float] = {}

        self._current_holdings_value: float = 0.0
        self._current_equity: float = initial_capital
        self._realized_pnl: float = 0.0

        # 记录权益曲线 [(timestamp, equity)]
        self._equity_curve: List[Tuple[datetime, float]] = []
        # 记录初始状态 (需要一个初始时间戳，可以设为 None 或在第一个事件时记录)
        self._last_timestamp: Optional[datetime] = None

        logger.info(f"BasicPortfolioManager 初始化: 初始资金 = ${initial_capital:.2f}")


    def on_event(self, event: Event):
        """根据事件类型分发处理。"""
        timestamp = getattr(event, 'timestamp', None)
        if timestamp:
             self._last_timestamp = timestamp # Track last event time

        if event.event_type == FillEvent.event_type:
            self._handle_fill(event)
            # 在 Fill 后立即更新一次市值和权益可能更准确
            self._update_portfolio_value(timestamp or datetime.now()) # Use fill time
            self._record_equity(timestamp or datetime.now())
            self._emit_portfolio_update(timestamp or datetime.now())

        elif event.event_type == MarketEvent.event_type:
            self._handle_market(event)
            # Market event drives regular equity updates
            self._record_equity(event.timestamp)
            self._emit_portfolio_update(event.timestamp)


    def _handle_fill(self, fill: FillEvent):
        """处理成交事件，更新现金和持仓。"""
        symbol = fill.symbol
        fill_qty = fill.fill_quantity
        fill_price = fill.fill_price
        commission = fill.commission
        direction_multiplier = 1 if fill.direction == 'BUY' else -1

        trade_cost = (fill_qty * fill_price * direction_multiplier) + abs(commission) # Cost for buy, proceeds for sell (sign adjusted)
        # More direct:
        if fill.direction == 'BUY':
             cost_change = - (fill_qty * fill_price + commission) # Cash decreases
        else: # SELL
             cost_change = (fill_qty * fill_price - commission) # Cash increases

        # 1. Update Cash
        self._current_cash += cost_change
        logger.debug(f"Fill 处理: {fill.direction} {fill_qty} {symbol} @ ${fill_price:.4f}, Comm: ${commission:.4f}. "
                     f"现金变化: ${cost_change:.2f}, 当前现金: ${self._current_cash:.2f}")

        # 2. Update Position
        current_position = self._positions.get(symbol)
        old_qty = current_position.quantity if current_position else 0.0
        old_avg_price = current_position.average_price if current_position else 0.0

        new_qty = old_qty + (fill_qty * direction_multiplier)

        # --- Realized PnL Calculation (occurs on sells that reduce/close position) ---
        fill_realized_pnl = 0.0
        if fill.direction == 'SELL' and old_qty > 0: # Selling part or all of a long position
            # Only calculate PnL on the quantity being sold from the existing long position
            qty_sold_from_long = min(fill_qty, old_qty)
            cost_basis_sold = qty_sold_from_long * old_avg_price
            proceeds = qty_sold_from_long * fill_price
            # Allocate commission proportionally if needed, here just use total fill commission
            # Simple PnL for this fill part:
            fill_realized_pnl = (proceeds - (commission * (qty_sold_from_long / fill_qty))) - cost_basis_sold
            self._realized_pnl += fill_realized_pnl
            logger.debug(f"    Realized PnL from this fill: ${fill_realized_pnl:.2f}. Total Realized PnL: ${self._realized_pnl:.2f}")
        # (Add logic for covering shorts if shorting is supported)


        # --- Update/Create/Remove Position Object ---
        if abs(new_qty) < 1e-9: # Position closed (handle floating point issues)
            if symbol in self._positions:
                del self._positions[symbol]
                logger.info(f"持仓关闭: {symbol}")
        else:
            if current_position is None: # New position
                new_avg_price = fill_price # Avg price is just the fill price
                pos = Position(symbol=symbol,
                               quantity=new_qty,
                               average_price=new_avg_price,
                               last_update_time=fill.timestamp)
                self._positions[symbol] = pos
                logger.info(f"新开仓位: {symbol}, Qty: {new_qty}, AvgPrice: ${new_avg_price:.4f}")
            else: # Update existing position
                if fill.direction == 'BUY':
                    # Update average price when buying more
                    new_avg_price = ((old_qty * old_avg_price) + (fill_qty * fill_price)) / new_qty
                else: # Selling, average price remains the same
                    new_avg_price = old_avg_price

                current_position.quantity = new_qty
                current_position.average_price = new_avg_price
                current_position.last_update_time = fill.timestamp
                logger.info(f"更新仓位: {symbol}, 新 Qty: {new_qty}, 新 AvgPrice: ${new_avg_price:.4f}")

        # Update latest market price from fill if needed (though market event is better)
        self._latest_market_prices[symbol] = fill_price


    def _handle_market(self, market: MarketEvent):
        """处理市场数据，更新最新价格和组合价值。"""
        symbol = market.symbol
        # Use close price for Mark-to-Market, could be configurable
        latest_price = market.close_price
        self._latest_market_prices[symbol] = latest_price

        # Update value for the specific position that received market data
        if symbol in self._positions:
            pos = self._positions[symbol]
            pos.market_price = latest_price
            pos.market_value = pos.quantity * latest_price
            pos.unrealized_pnl = (latest_price - pos.average_price) * pos.quantity
            # logger.debug(f"Market Update for {symbol}: Price=${latest_price:.2f}, MktVal=${pos.market_value:.2f}, UnrPnL=${pos.unrealized_pnl:.2f}")

        # Recalculate total holdings value
        self._update_portfolio_value(market.timestamp)


    def _update_portfolio_value(self, timestamp: datetime):
        """根据最新的市场价格重新计算总持仓市值和总权益。"""
        total_holdings_value = 0.0
        for symbol, pos in self._positions.items():
            # Use the last known market price for each position
            last_price = self._latest_market_prices.get(symbol, pos.average_price) # Fallback to avg price if no market data yet
            if last_price != pos.market_price: # Update if needed
                 pos.market_price = last_price
                 pos.market_value = pos.quantity * pos.market_price
                 pos.unrealized_pnl = (last_price - pos.average_price) * pos.quantity

            total_holdings_value += pos.market_value

        self._current_holdings_value = total_holdings_value
        self._current_equity = self._current_cash + self._current_holdings_value
        # logger.debug(f"Portfolio Value Update @ {timestamp}: Holdings=${self._current_holdings_value:.2f}, Equity=${self._current_equity:.2f}")


    def _record_equity(self, timestamp: datetime):
        """记录当前权益到时间序列。"""
        if timestamp is None: return # Cannot record without a timestamp
        # Avoid duplicate entries for the same timestamp if events arrive close together
        if not self._equity_curve or self._equity_curve[-1][0] != timestamp:
            self._equity_curve.append((timestamp, round(self._current_equity, 4)))
            # logger.debug(f"Equity recorded @ {timestamp}: ${self._current_equity:.2f}")


    def _emit_portfolio_update(self, timestamp: datetime):
        """生成并发送 PortfolioUpdateEvent。"""
        update_event = PortfolioUpdateEvent(
            timestamp=timestamp,
            portfolio_id="main", # Or make this configurable
            total_value=round(self._current_equity, 4),
            cash=round(self._current_cash, 4),
            positions=self.get_all_positions(), # Get a copy of current positions
            # Optionally add realized/unrealized PnL etc.
        )
        self.event_engine.put(update_event)


    # --- Public Query Methods ---
    def get_position(self, symbol: str) -> Optional[Position]:
        # Return a copy to prevent external modification? For now, return direct reference.
        return self._positions.get(symbol)

    def get_all_positions(self) -> Dict[str, Position]:
        # Return a copy
        return self._positions.copy()

    def get_cash(self) -> float:
        return round(self._current_cash, 4)

    def get_current_holdings_value(self) -> float:
        # Recalculate just in case? Or rely on internal state? Relying is faster.
        # self._update_portfolio_value(self._last_timestamp or datetime.now()) # Force update? Maybe not needed.
        return round(self._current_holdings_value, 4)

    def get_current_equity(self) -> float:
        # self._update_portfolio_value(self._last_timestamp or datetime.now()) # Force update? Maybe not needed.
        return round(self._current_equity, 4)

    def get_equity_curve(self) -> List[Tuple[datetime, float]]:
        # Return a copy
        return self._equity_curve[:]