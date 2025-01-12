import akshare as ak
import talib as ta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn.metrics import classification_report
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier  # Import XGBClassifier
from sklearn.feature_selection import RFE
from tensorflow import keras
from tensorflow.keras import layers
from keras.models import Sequential, Model
from keras.layers import Conv1D, MaxPooling1D, Flatten, Dense, Input
from tensorflow.keras.callbacks import EarlyStopping
import os
from gplearn.genetic import SymbolicRegressor
from sklearn.feature_selection import SelectKBest, f_classif  # For classification
import warnings

# 忽略所有 UserWarning 类型的警告
warnings.filterwarnings('ignore', category=UserWarning)

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

def create_sharpe_labels_forward(df, window=5, risk_free_rate=0.02, threshold=0):
    df = df.copy()

    df['returns'] = df['close'].pct_change(window)

    # Calculate FORWARD-looking Sharpe Ratio
    df['sharpe_ratio_future'] = (df['returns'].rolling(window, min_periods=1).mean() - (risk_free_rate/252)) / df['returns'].rolling(window, min_periods=1).std() * np.sqrt(252)
    df['sharpe_ratio_future'] = df['sharpe_ratio_future'].shift(-window+1) # shift up by window -1
    # Correct labeling (avoiding lookahead bias): Label based on the *future* Sharpe ratio
    df['target'] = (df.returns.shift(1) > 0).astype(int)

    df.dropna(inplace=True)

    return df


def add_technical_indicators(df):
    """Adds common technical indicators using TA-Lib."""
    df['sma_5'] = ta.SMA(df.close, timeperiod=5)
    df['sma_10'] = ta.SMA(df.close, timeperiod=10)
    df['sma_20'] = ta.SMA(df.close, timeperiod=20)
    df['rsi_7'] = ta.RSI(df.close, timeperiod=7)  # Common RSI period is 14
    df['rsi_14'] = ta.RSI(df.close, timeperiod=14)  # Common RSI period is 14
    df['obv'] = ta.OBV(df.close, df.volume)
    macd, macdsignal, macdhist = ta.MACD(df.close, fastperiod=12, slowperiod=26, signalperiod=9)
    df['macd'] = macd
    df['macdsignal'] = macdsignal
    df['macdhist'] = macdhist
    df['ema_5'] = ta.EMA(df.close, timeperiod=5)
    df['ema_12'] = ta.EMA(df.close, timeperiod=12)
    df['ema_26'] = ta.EMA(df.close, timeperiod=26)
    df['vol_5'] = df['close'].rolling(window=5).std()
    df['vol_10'] = df['close'].rolling(window=10).std()
    df['vol_20'] = df['close'].rolling(window=20).std()

    # Momentum Indicators
    df['momentum_10'] = ta.MOM(df.close, timeperiod=10)  # 10-period momentum
    df['momentum_20'] = ta.MOM(df.close, timeperiod=20)  # 10-period momentum
    df['momentum_60'] = ta.MOM(df.close, timeperiod=60)  # 10-period momentum
    df['roc_10'] = ta.ROC(df.close, timeperiod=10)      # 10-period Rate of Change
    df['roc_30'] = ta.ROC(df.close, timeperiod=30)      # 10-period Rate of Change
    df['roc_60'] = ta.ROC(df.close, timeperiod=60)      # 10-period Rate of Change

    df['ppo'] = ta.PPO(df.close, fastperiod=12, slowperiod=26) #Percentage Price Oscillator
    df['willr'] = ta.WILLR(df.high, df.low, df.close, timeperiod=14) #William's %R
    return df

def get_signal_df(futures_etf, name = 'C0', N = 5, lookback = 500):
    x_df = futures_etf.copy()
    x_df = x_df.dropna()
    x_df = add_technical_indicators(x_df)
    x_df = create_sharpe_labels_forward(x_df, window=N)
    x_df = x_df.dropna() # Drop rows with NaN introduced by label creation
    
    # Split into X and y
    X = x_df[['sma_5', 'sma_10', 'sma_20', 'rsi_7', 'rsi_14', 'obv', 'macd',
           'macdsignal', 'macdhist', 'ema_5', 'ema_12', 'ema_26', 'momentum_10', 'momentum_20', 'momentum_60',
           'roc_10', 'roc_30', 'roc_60', 'ppo', 'willr', 'open', 'high', 'low', 'volume', 'vol_5', 'vol_10', 'vol_20']]
    y = x_df['target']
    
   
    predictions = []
    n_features = X.shape[1]

    # model = XGBClassifier(
    #     n_estimators=400, 
    #     max_depth=2, 
    #     eval_metric='logloss',
    #     objective='binary:logistic',
    #     random_state=20
    # )
    # model = SVC(C=1, kernel='rbf')
    model = MLPClassifier(random_state=1, max_iter=300, hidden_layer_sizes=(100, 25))

    for i in range(lookback, len(x_df)):
        train_index = range(i - lookback, i, N)
        X_train_window = X.iloc[train_index]
        y_train_window = y.iloc[train_index]  # 使用整个窗口的标签

        selector = SelectKBest(f_classif, k=N)  # Select N features based on F-value test
        X_train_window = selector.fit_transform(X_train_window, y_train_window)
        selected_features = X.columns[selector.get_support(indices=True)] 

        scaler = StandardScaler()
        X_train_window = scaler.fit_transform(X_train_window)

        model.fit(X_train_window, y_train_window)  # Adjust epochs and batch size as needed
        
        X_test = X.loc[[X.index[i]], selected_features] # Use .loc and get index label
        X_test_scaled = scaler.transform(X_test)
     
        y_pred = model.predict(X_test_scaled)
        predictions.append({
            'name': name,
            'date': X_test.index[0],
            'signal': y_pred[0]
        })  # Append the prediction

    y_pred_df = pd.DataFrame(predictions)
    y_pred_df = y_pred_df.set_index('date')
    return y_pred_df

if __name__ == "__main__":
    corn_df = ak.futures_zh_daily_sina(symbol="C0")
    apple_df = ak.futures_zh_daily_sina(symbol="MA0")
    methanol_df = ak.futures_zh_daily_sina(symbol="HC0")

    corn_df_c = corn_df.set_index('date')  # Assign the result back to corn_df
    apple_df_c = apple_df.set_index('date') # Assign the result back to apple_df
    methanol_df_c = methanol_df.set_index('date')
    
    
    methanol_df_align = methanol_df_c.copy()
    corn_df_align = corn_df_c.reindex(methanol_df_align.index)
    apple_df_align = apple_df_c.reindex(methanol_df_align.index)

    print('training...')
    corn_signal_df = get_signal_df(corn_df_align, 'C0')
    apple_signal_df = get_signal_df(apple_df_align, 'AP0')
    methanol_signal_df = get_signal_df(methanol_df_align, 'M0')

    corn_pivot_df = corn_signal_df.pivot(columns='name', values='signal')
    apple_pivot_df = apple_signal_df.pivot(columns='name', values='signal')
    methanol_pivot_df = methanol_signal_df.pivot(columns='name', values='signal')
    
    
    close_df = pd.concat([
        corn_df_align[['close']].rename(columns={ 'close': 'C0' }),
        apple_df_align[['close']].rename(columns={ 'close': 'AP0' }),
        methanol_df_align[['close']].rename(columns={ 'close': 'M0' })
    ], axis=1)
    
    all_df = pd.concat([corn_pivot_df, apple_pivot_df, methanol_pivot_df], axis=1)

    print('data merging...')
    print('model train done')
    all_df.to_csv('backtest_signal.csv')
    close_df.to_csv('backtest_price.csv')
