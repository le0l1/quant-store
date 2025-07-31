import akshare as ak
import pandas as pd

def get_etf_data(etf_code):
    """
    获取 ETF 的历史数据
    """
    try:
        df = ak.fund_etf_hist_em(symbol=etf_code, adjust="qfq")
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        # 列名统一转为小写，方便处理
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        print(f"获取 {etf_code} 数据失败: {e}")
        return None

def calculate_indicators(df):
    """
    计算所需的技术指标
    """
    # 20日动量
    df['momentum_20'] = df['收盘'].diff(20)
    # 200日移动平均线
    df['ma_200'] = df['收盘'].rolling(window=200).mean()
    # 13日动量
    df['momentum_13'] = df['收盘'].diff(13)
    return df

def run_strategy():
    """
    执行ETF动量策略
    """
    etfs = ['561300', '159726', '515100', '513500', '161119', '518880', '164824', '159985', '513330', '513100', '513030', '513520']
    
    eligible_etfs = []

    print("开始获取和处理ETF数据...")
    for etf_code in etfs:
        print(f"处理ETF: {etf_code}")
        data = get_etf_data(etf_code)
        if data is None or data.empty:
            continue
        
        data = calculate_indicators(data)
        
        # 获取最新数据
        latest_data = data.iloc[-1]
        
        # 过滤条件
        # 1. 20日动量 > 0
        # 2. 收盘价 > 200MA
        if latest_data['momentum_20'] > 0 and latest_data['收盘'] > latest_data['ma_200']:
            eligible_etfs.append({
                'code': etf_code,
                'momentum_13': latest_data['momentum_13']
            })
            print(f"{etf_code} 符合过滤条件。")
        else:
            print(f"{etf_code} 不符合过滤条件。")


    if not eligible_etfs:
        print("没有符合条件的ETF。")
        return

    # 根据13日动量从大到小排序
    eligible_etfs.sort(key=lambda x: x['momentum_13'], reverse=True)
    
    print("\n筛选出的ETF (按13日动量排序):")
    for etf in eligible_etfs:
        print(f"代码: {etf['code']}, 13日动量: {etf['momentum_13']:.2f}")

    # 进场条件: 取前三
    top_3_etfs = eligible_etfs[:3]
    
    print("\n最终选择的ETF (Top 3):")
    if not top_3_etfs:
        print("无")
    else:
        for etf in top_3_etfs:
            print(f"代码: {etf['code']}")

if __name__ == "__main__":
    run_strategy()
