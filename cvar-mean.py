import numpy as np
import pandas as pd
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import adata
import json
from datetime import datetime, timedelta


def calculate_returns(prices):
    return prices.pct_change().dropna()

def portfolio_return(weights, returns):
    """Calculates portfolio return."""
    return np.sum(returns.mean() * weights)

def calculate_cvar(returns, confidence_level=0.05):
    """Calculates Conditional Value at Risk (CVaR)."""
    sorted_returns = np.sort(returns)
    alpha_index = int(np.floor((1 - confidence_level) * len(returns)))
    cvar = -np.mean(sorted_returns[:alpha_index])
    return cvar

def portfolio_cvar(weights, returns, confidence_level=0.05):
    """Calculates portfolio CVaR."""
    portfolio_returns = returns.dot(weights)
    return calculate_cvar(portfolio_returns, confidence_level)

def mean_cvar_optimization(returns, target_return, confidence_level=0.05):
    """Optimizes portfolio for Mean-CVaR given a target return."""
    num_assets = returns.shape[1]
    args = (returns, confidence_level)
    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1}, # Weights sum to 1
                   {'type': 'eq', 'fun': lambda x: portfolio_return(x, returns) - target_return}) #Target Return constraint
    bounds = tuple((0.03, 1) for asset in range(num_assets))  # Weights between 0.05 and 1
    initial_weights = np.array([1/num_assets]*num_assets)
    result = minimize(portfolio_cvar, initial_weights, args=args, method='SLSQP', bounds=bounds, constraints=constraints)
    return pd.Series(result.x, index=returns.columns)

def backtest_strategy(df, window_size=200, period=20, target_return_annualized=0.10, confidence_level=0.05):
    """Backtests the Mean-CVaR strategy with a target return."""

    p_df = df.iloc[::-period].iloc[::-1].dropna()
    all_daily_returns = []
    weights_list = []

    start_at = max(0, len(p_df) - len(df) // window_size)
    for i in range(start_at, len(p_df)):
        current_date = p_df.index[i]
        start_idx = df.index.get_loc(current_date) - window_size + 1
        if start_idx < 0:
            continue
        
        historical_data = df.iloc[start_idx:start_idx + window_size]
        returns = calculate_returns(historical_data)

        # Convert annualized target return to daily target return
        trading_days_per_year = 252 # Adjust if needed
        target_return_daily = (1 + target_return_annualized)**(1/trading_days_per_year) - 1
        
        weights = mean_cvar_optimization(returns, target_return=target_return_daily, confidence_level=confidence_level)
        weights = weights.round(2)
        weights_list.append(pd.Series(weights, name=current_date, index=returns.columns))
        
    weight_df = pd.concat(weights_list, axis=1)
    
    return weight_df

def read_end_date_from_file():
    with open('end_date.txt', 'r') as f:
        return f.read().strip()

def write_end_date_to_file(end_date):
    with open('end_date.txt', 'w') as f:
        f.write(end_date)

if __name__ == "__main__":
    etfs = ['561300', '159726', '515100', '513500', '511260', '518880', '164824'] 
    df = pd.DataFrame()
    today_date = datetime.today().strftime('%Y-%m-%d')

    end_date = read_end_date_from_file()

    N = 20  # Set your threshold for days
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

    print(end_date)
    trade_calendar = adata.stock.info.trade_calendar().query('trade_status == "1"')
    next_trade_index = trade_calendar[trade_calendar.trade_date == end_date].index + N
    next_trade_day = trade_calendar.iloc[next_trade_index].trade_date
    
    if next_trade_day.values[0] != today_date:
        print(f'交易周期小于{N} days')
        exit() 
    
    # Calculate start_date as 200 days before the end_date
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    start_date = (end_date_dt - timedelta(days=200)).strftime('%Y-%m-%d')

    write_end_date_to_file(today_date)
    
    for i in etfs:
        etf_df = adata.fund.market.get_market_etf(i, start_date=start_date, end_date=end_date, k_type=1)
        etf_df['close'] = etf_df['close'].astype(float)
        pivot_df = etf_df.pivot(index='trade_date', columns='fund_code', values='close')
        df = pd.concat([df, pivot_df], axis=1)

    weight_df = backtest_strategy(
        e_df.dropna(), 
        window_size=120, 
        period=30, 
        target_return_annualized=0.08, 
        confidence_level=0.05
    )

    print(f'Today: {today_date}')
    print('Latest Weights:')
    print(weight_df.T.tail(10))
