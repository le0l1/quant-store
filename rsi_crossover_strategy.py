# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement, duplicate-code, unused-argument, attribute-defined-outside-init
# flake8: noqa: F401 F821 W503 E501 F403 F405 E402
# isort: skip_file
# --- Do not remove these imports ---
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta, timezone
from typing import Optional, Union
import logging # Import logging

from freqtrade.strategy import (
    IStrategy,
    Trade,
    Order,
    PairLocks,
    informative,
    BooleanParameter,
    CategoricalParameter,
    DecimalParameter,
    IntParameter,
    RealParameter,
    timeframe_to_minutes,
    timeframe_to_next_date,
    timeframe_to_prev_date,
    merge_informative_pair,
    stoploss_from_absolute,
    stoploss_from_open,
)
# Import wallet access
from freqtrade.persistence import Trade, LocalTrade
from freqtrade.exchange import timeframe_to_prev_date

# --------------------------------
# Add your lib to import here
import talib.abstract as ta
from technical import qtpylib
# --------------------------------

logger = logging.getLogger(__name__) # Add logger

class BBandRsiBreakoutStrategy(IStrategy): # Renamed class back
    """
    Strategy combining Bollinger Band Breakouts with RSI confirmation.
    Includes dynamic position sizing to risk a fixed percentage of equity per trade.

    Entry Long: Close > Upper BBand AND RSI > Confirmation Level
    Exit Long:  Close < Middle BBand OR RSI < Exit Level OR Stoploss hit OR ROI hit

    Entry Short: Close < Lower BBand AND RSI < Confirmation Level
    Exit Short:  Close > Middle BBand OR RSI > Exit Level OR Stoploss hit OR ROI hit

    Uses a fixed percentage stoploss defined in `stoploss`.
    Position size is calculated dynamically based on `risk_per_trade`.
    Exits are based on Bollinger middle band cross, RSI levels, stoploss, or ROI.
    """
    INTERFACE_VERSION = 3

    # Can this strategy go short?
    can_short: bool = True

    # --- Strategy Configuration ---
    # Minimal ROI configuration (example, can be optimized)
    minimal_roi = {
        "60": 0.03,  # 3% profit after 60 minutes
        "30": 0.02,  # 2% profit after 30 minutes
        "0": 0.05    # 5% profit immediately (or minimum profit target)
    }

    # Stoploss Requirement: This value is CRUCIAL for position sizing.
    # It MUST be negative and represent the max % loss from entry.
    stoploss = -0.05 # Example: Rise an 8% stoploss

    # Trailing stoploss (optional)
    trailing_stop = False
    # trailing_stop_positive = 0.01
    # trailing_stop_positive_offset = 0.03
    # trailing_only_offset_is_reached = True

    # --- Dynamic Stake Configuration ---
    # Set risk per trade (e.g., 0.01 for 1%)
    risk_per_trade: float = 0.02 # Risk 1% of total equity per trade

    # Optimal timeframe for the strategy.
    timeframe = '4h'

    # Run "populate_indicators()" only for new candle.
    process_only_new_candles = True

    # These values can be overridden in the config.
    use_exit_signal = True # Use signals from populate_exit_trend
    exit_profit_only = False # Allow exits at a loss based on signals/SL
    ignore_roi_if_entry_signal = False # Consider ROI targets

    # --- Hyperoptable Parameters ---

    # Bollinger Bands
    bb_window = IntParameter(low=10, high=50, default=20, space="buy", optimize=True, load=True)
    bb_stddev = RealParameter(low=1.0, high=3.0, default=2.0, space="buy", optimize=True, load=True)

    # RSI
    rsi_period = IntParameter(low=7, high=25, default=14, space="buy", optimize=True, load=True)
    buy_rsi_confirmation = IntParameter(low=50, high=70, default=55, space="buy", optimize=True, load=True)
    short_rsi_confirmation = IntParameter(low=30, high=50, default=45, space="sell", optimize=True, load=True)

    # RSI Exit Levels (Re-added)
    sell_rsi_exit = IntParameter(low=40, high=55, default=48, space="sell", optimize=True, load=True)
    exit_short_rsi_exit = IntParameter(low=45, high=60, default=52, space="buy", optimize=True, load=True)

    # --- Startup Settings ---
    # Should be at least max(bb_window.default, rsi_period.default)
    startup_candle_count: int = 30

    # --- Order Settings ---
    order_types = {
        'entry': 'limit',
        'exit': 'limit', # Exit based on ROI or exit signal
        'stoploss': 'market', # Market stoploss ensures execution
        'stoploss_on_exchange': False # Calculate SL off-exchange initially
    }

    order_time_in_force = {
        'entry': 'GTC',
        'exit': 'GTC',
    }

    # --- Plotting ---
    plot_config = {
        'main_plot': {
            'bb_upperband': {'color': 'grey', 'plotly': {'opacity': 0.5}},
            'bb_middleband': {'color': 'grey'},
            'bb_lowerband': {'color': 'grey', 'plotly': {'opacity': 0.5}},
        },
        'subplots': {
            "RSI": {
                'rsi': {'color': 'red'},
                'buy_rsi_conf_line': {'color': 'lightgreen', 'plotly': {'opacity': 0.4}},
                'short_rsi_conf_line': {'color': 'lightcoral', 'plotly': {'opacity': 0.4}},
                # Re-add RSI exit lines to plot
                'sell_rsi_exit_line': {'color': 'orange', 'plotly': {'opacity': 0.4}},
                'exit_short_rsi_exit_line': {'color': 'lightblue', 'plotly': {'opacity': 0.4}},
            },
        },
    }

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float, entry_tag: str | None, side: str,
                 **kwargs) -> float:
        return 5.0

    # --- Custom Stake Amount Calculation (Unchanged) ---
    def custom_stake_amount(self, pair: str, current_time: datetime, current_rate: float,
                            proposed_stake: float, min_stake: Optional[float], max_stake: float,
                            leverage: float, entry_tag: Optional[str], side: str,
                            **kwargs) -> float:
        """
        Calculate stake amount to risk a fixed percentage of total equity.
        (Code remains the same as the previous versions)
        """
        # Ensure stoploss is negative, as expected by Freqtrade and this calculation
        if self.stoploss >= 0:
             logger.warning(f"Stoploss ({self.stoploss}) must be negative for risk calculation. Defaulting to proposed_stake.")
             if min_stake is not None and proposed_stake < min_stake:
                 return 0.0
             return proposed_stake


        # Use abs() for calculation, representing stop distance %
        stoploss_pct = abs(self.stoploss)
        if stoploss_pct == 0:
            logger.warning("Stoploss is zero, cannot calculate risk-based stake. Defaulting to proposed_stake.")
            if min_stake is not None and proposed_stake < min_stake:
                 return 0.0
            return proposed_stake

        # Get total equity in stake currency
        try:
            if self.wallets is None:
                logger.warning("Wallets object is not available. Cannot calculate dynamic stake.")
                # Fallback to proposed_stake if wallets are not ready (e.g., during startup)
                if min_stake is not None and proposed_stake < min_stake:
                    return 0.0
                return proposed_stake

            stake_currency = self.config['stake_currency']
            total_equity = self.wallets.get_total(stake_currency)
            available_balance = self.wallets.get_available(stake_currency) # Check available too

            if total_equity <= 0:
                 logger.warning(f"Total equity is zero or negative ({total_equity}). Cannot place trade.")
                 return 0.0

        except Exception as e:
            logger.warning(f"Could not get wallet balance: {e}. Defaulting to proposed_stake.")
            if min_stake is not None and proposed_stake < min_stake:
                 return 0.0
            return proposed_stake

        # Calculate maximum amount to risk in stake currency
        max_risk_amount = total_equity * self.risk_per_trade

        # Calculate stake based purely on risk and stoploss percentage
        # stake = RiskAmount / StopLossPercent
        calculated_stake = max_risk_amount / stoploss_pct

        # --- Apply Capital Constraints ---
        # Max stake based on available balance
        stake = min(calculated_stake, available_balance)

        # Max stake based on max_stake (usually inf, but respects config)
        stake = min(stake, max_stake)

        # Max stake based on potential overallocatiion if strategy is aggressive
        # (Optional: Consider tradable_balance_ratio or max_open_trades division)
        # Example: Simple division by max_open_trades
        max_open_trades = self.config.get('max_open_trades', 1)
        if max_open_trades > 0:
             # Consider total equity / max_open_trades as a cap per slot
             max_stake_per_slot = total_equity / max_open_trades
             stake = min(stake, max_stake_per_slot)


        # --- Apply Exchange / Bot Limits ---
        # Ensure stake is not less than exchange minimum.
        if min_stake is not None and stake < min_stake:
            # Option 1: Skip trade if calculated risk is too small for min_stake
            # logger.info(f"Calculated stake {stake:.2f} for {pair} is below min_stake {min_stake}. Skipping trade.")
            # return 0.0

            # Option 2: Use min_stake if affordable, accepting slightly higher risk %
            if min_stake <= available_balance and min_stake <= max_stake: # Check affordability
                 logger.warning(f"Calculated stake {stake:.2f} for {pair} is below min_stake {min_stake}. "
                                f"Using min_stake, actual risk may exceed {self.risk_per_trade*100:.2f}%.")
                 stake = min_stake
            else:
                 logger.info(f"Calculated stake {stake:.2f} is below min_stake {min_stake}, and min_stake is not affordable. Skipping trade for {pair}.")
                 return 0.0 # Cannot afford even min_stake

        # Final check if we ended up with zero or negative stake
        if stake <= 0:
             logger.warning(f"Final stake calculation resulted in {stake:.2f} for {pair}. Skipping trade.")
             return 0.0

        # logger.info(f"Risk calculation for {pair} ({side}): Total Equity={total_equity:.2f}, Risk %={self.risk_per_trade*100}%, "
        #             f"SL %={stoploss_pct*100}%, Calculated Stake={calculated_stake:.2f}, Final Stake={stake:.2f}")

        return stake


    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """ Adds Bollinger Bands, RSI, and Fibonacci retracement levels """
        # --- Bollinger Bands ---
        bollinger = qtpylib.bollinger_bands(
            qtpylib.typical_price(dataframe), # Use typical price for BBands
            window=self.bb_window.value,
            stds=self.bb_stddev.value
        )
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_middleband'] = bollinger['mid']
        dataframe['bb_upperband'] = bollinger['upper']

        # --- RSI ---
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=self.rsi_period.value)

        # --- 添加长期趋势指标 ---
        dataframe['ema_long'] = ta.EMA(dataframe, timeperiod=200)  # 200周期EMA作为长期趋势判断
        
        # --- Add lines for plot config ---
        dataframe['buy_rsi_conf_line'] = self.buy_rsi_confirmation.value
        dataframe['short_rsi_conf_line'] = self.short_rsi_confirmation.value
        dataframe['sell_rsi_exit_line'] = self.sell_rsi_exit.value
        dataframe['exit_short_rsi_exit_line'] = self.exit_short_rsi_exit.value

        return dataframe

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float, entry_tag: str | None, side: str,
                 **kwargs) -> float:
        return 5.0

    # --- Fibonacci Retracement Levels ---
        high = dataframe['high'].rolling(window=self.bb_window.value).max()
        low = dataframe['low'].rolling(window=self.bb_window.value).min()


        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """ Populates enter signals based on BBand Breakout, RSI confirmation, and Fibonacci retracement levels with pullback after breakout """
        # --- Long Entry Conditions ---
        dataframe.loc[
            (
                (dataframe['close'] > dataframe['bb_upperband']) & # Pullback below breakout high
                (dataframe['rsi'] > self.buy_rsi_confirmation.value) &
                (dataframe['volume'] > 0) # Basic sanity check
            ),
            'enter_long'] = 1

        # --- Short Entry Conditions ---
        dataframe.loc[
            (
                (dataframe['close'] < dataframe['bb_lowerband']) & # Pullback above breakout low
                (dataframe['rsi'] < self.short_rsi_confirmation.value) &
            
                (dataframe['volume'] > 0) # Basic sanity check
            ),
            'enter_short'] = 1

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """ Populates exit signals based on BB middle band cross """
        # --- Long Exit Conditions ---
        dataframe.loc[
            (
                (dataframe['close'] < dataframe['bb_middleband']) & # Close crosses below middle band
                (dataframe['volume'] > 0) # Basic sanity check
            ),
            'exit_long'] = 1

        # --- Short Exit Conditions ---
        dataframe.loc[
            (
                (dataframe['close'] > dataframe['bb_middleband']) & # Close crosses above middle band
                (dataframe['volume'] > 0) # Basic sanity check
            ),
            'exit_short'] = 1

        return dataframe
