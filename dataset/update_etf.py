
import akshare as ak
import duckdb
import pandas as pd
from datetime import datetime, date

# List of ETF symbols to update
SYMBOLS = ['561300', '159726', '515100', '513500', '161119', '518880', '164824', '159985', '513330', '513100', '513030', '513520']
DB_FILE = 'quant_data.duckdb'
TABLE_NAME = 'etf_prices'

def get_latest_date(con, symbol):
    """Get the latest date for a given symbol from the database."""
    try:
        result = con.execute(f"SELECT MAX(日期) FROM {TABLE_NAME} WHERE symbol = '{symbol}'").fetchone()
        return result[0] if result and result[0] else '20000101'
    except duckdb.CatalogException:
        # Table doesn't exist yet
        return '20000101'

def update_etf_data():
    """
    Fetches ETF price data using akshare and stores it in a DuckDB database.
    """
    con = duckdb.connect(DB_FILE)

    # Create table if it doesn't exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            日期 DATE,
            开盘 DOUBLE,
            收盘 DOUBLE,
            最高 DOUBLE,
            最低 DOUBLE,
            成交量 BIGINT,
            成交额 DOUBLE,
            振幅 DOUBLE,
            涨跌幅 DOUBLE,
            涨跌额 DOUBLE,
            换手率 DOUBLE,
            symbol VARCHAR,
            PRIMARY KEY (日期, symbol)
        );
    """)

    today = datetime.now()
    today_for_akshare = today.strftime('%Y%m%d')

    for symbol in SYMBOLS:
        latest_date = get_latest_date(con, symbol) # date object or '20000101'

        if isinstance(latest_date, date):
            start_date_for_sql = latest_date.strftime('%Y-%m-%d')
            start_date_for_akshare = latest_date.strftime('%Y%m%d')
        else: # string '20000101'
            start_date_for_akshare = latest_date
            start_date_for_sql = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"

        print(f"Fetching data for {symbol} from {start_date_for_akshare} to {today_for_akshare}...")

        try:
            # Fetch historical ETF data
            etf_hist_df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date_for_akshare, end_date=today_for_akshare, adjust="hfq")

            if etf_hist_df.empty:
                print(f"No new data found for symbol {symbol}.")
                continue

            # Add symbol column
            etf_hist_df['symbol'] = symbol
            
            # Convert '日期' to datetime objects for proper handling
            etf_hist_df['日期'] = pd.to_datetime(etf_hist_df['日期']).dt.date

            # Insert data into DuckDB
            con.execute(f"DELETE FROM {TABLE_NAME} WHERE symbol = '{symbol}' AND 日期 >= '{start_date_for_sql}'")
            con.register('etf_df_temp', etf_hist_df)
            con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM etf_df_temp")
            print(f"Successfully updated data for symbol {symbol}.")

        except Exception as e:
            print(f"An error occurred while fetching data for symbol {symbol}: {e}")

    con.close()

if __name__ == "__main__":
    update_etf_data()
