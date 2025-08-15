#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据更新脚本 - 使用分区Parquet文件存储
支持每日数据获取并按月合并到Parquet分区。
"""

import akshare as ak
import pandas as pd
import os
import logging
from datetime import datetime, timedelta

# --- 配置 ---
# 要更新的ETF符号列表
SYMBOLS = ['561300', '159726', '515100', '513500', '161119', '518880', '164824', '159985', '513330', '513100', '513030', '513520']
# Parquet文件的根目录
OUTPUT_DIR = 'data'
# 数据源的表名，将用作输出目录的一部分
TABLE_NAME = 'etf_prices'
# 用于分区的日期列
DATE_COLUMN = 'date'
# 用于唯一识别记录的列，在合并数据时去重
UNIQUE_COLUMNS = ['date', 'symbol']
# --- 配置结束 ---

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_update.log', mode='a'), # 追加模式
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def save_data_to_parquet(df: pd.DataFrame, output_dir: str, table_name: str):
    """
    将DataFrame保存到分区的Parquet文件，并处理合并逻辑。

    Args:
        df: 包含新数据的DataFrame。
        output_dir: Parquet文件的根目录。
        table_name: 表名，用于创建子目录。
    """
    if df.empty:
        logger.warning("输入的数据为空，无需保存。")
        return

    if DATE_COLUMN not in df.columns:
        logger.error(f"数据中缺少指定的日期列 '{DATE_COLUMN}'，无法进行分区保存。")
        return

    # 确保日期列是datetime类型
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])

    # 创建 'year_month' 分区列
    df['year_month'] = df[DATE_COLUMN].dt.strftime('%Y-%m')
    logger.info(f"为 {table_name} 数据创建了 'year_month' 分区列。")

    # 按月份对新数据进行分组，以便逐月处理
    for year_month, group_df in df.groupby('year_month'):
        partition_path = os.path.join(output_dir, table_name, f"year_month={year_month}")
        os.makedirs(partition_path, exist_ok=True)
        
        file_path = os.path.join(partition_path, 'data.parquet')
        
        logger.info(f"--- 正在处理分区: {partition_path} ---")

        try:
            if os.path.exists(file_path):
                logger.info(f"发现现有数据文件: {file_path}，开始执行合并操作。")
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, group_df], ignore_index=True)
                merged_df = combined_df.drop_duplicates(subset=UNIQUE_COLUMNS, keep='last')
                logger.info(f"合并完成: 旧记录数={len(existing_df)}, 新记录数={len(group_df)}, 合并后总数={len(merged_df)}")
                final_df = merged_df
            else:
                logger.info("未发现现有数据，将直接写入新数据。")
                final_df = group_df

            final_df.to_parquet(file_path, compression='zstd', index=False)
            logger.info(f"✅ 成功将 {len(final_df)} 条记录写入到: {file_path}")

        except Exception as e:
            logger.error(f"❌ 处理分区 {partition_path} 时发生错误: {e}")

def update_etf_data():
    """
    使用akshare获取ETF价格数据并存储到分区的Parquet文件。
    """
    # 为了确保能覆盖到所有最近的更新，我们获取过去一年的数据
    # 合并逻辑将处理掉重复的数据
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')

    all_etf_df = pd.DataFrame()

    for symbol in SYMBOLS:
        logger.info(f"--- 开始获取ETF: {symbol} ---")
        try:
            etf_hist_df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")

            if etf_hist_df.empty:
                logger.warning(f"未能获取到 {symbol} 的数据。")
                continue

            # 重命名列为英文
            column_mapping = {
                '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
                '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                '涨跌幅': 'change_pct', '涨跌额': 'change_amount', '换手率': 'turnover_rate'
            }
            etf_hist_df.rename(columns=column_mapping, inplace=True)

            etf_hist_df['symbol'] = symbol
            all_etf_df = pd.concat([all_etf_df, etf_hist_df], ignore_index=True)
            logger.info(f"成功获取 {len(etf_hist_df)} 条 {symbol} 的数据。")

        except Exception as e:
            logger.error(f"获取 {symbol} 数据时发生错误: {e}")
    
    if not all_etf_df.empty:
        logger.info(f"\n开始将所有获取到的ETF数据写入Parquet...")
        # 调用核心函数，保存并合并数据
        save_data_to_parquet(all_etf_df, OUTPUT_DIR, TABLE_NAME)
        logger.info(f"所有ETF数据处理完毕。")
    else:
        logger.warning("未能获取到任何ETF数据，本次未写入任何文件。")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("开始执行ETF数据更新任务 - Parquet版本")
    logger.info("=" * 60)
    update_etf_data()
    logger.info("\nETF数据更新任务执行完毕。")