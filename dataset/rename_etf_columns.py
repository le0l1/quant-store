#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性脚本：重命名 etf_prices 数据集中所有 Parquet 文件的列名。
(修正版：可处理所有 .parquet 文件)
"""

import os
import pandas as pd
import logging

# --- 配置 ---
# ETF Parquet 文件的根目录
ETF_DATA_ROOT = 'data/etf_prices'

# 列名映射关系
COLUMN_MAPPING = {
    '日期': 'date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'turnover',
    '振幅': 'amplitude',
    '涨跌幅': 'change_pct',
    '涨跌额': 'change_amount',
    '换手率': 'turnover_rate'
}
# ---

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def rename_columns_in_parquet_files(root_dir):
    """
    遍历指定目录下的所有 .parquet 文件，并重命名其列。
    """
    logger.info(f"开始扫描目录: {root_dir}")
    files_processed = 0

    for subdir, _, files in os.walk(root_dir):
        for filename in files:
            # 修正：检查所有以 .parquet 结尾的文件
            if filename.endswith('.parquet'):
                file_path = os.path.join(subdir, filename)
                logger.info(f"--- 正在处理文件: {file_path} ---")
                try:
                    df = pd.read_parquet(file_path)
                    
                    # 检查是否需要重命名
                    if not any(col in COLUMN_MAPPING for col in df.columns):
                        logger.info("列名已经是英文，跳过此文件。" )
                        continue

                    df.rename(columns=COLUMN_MAPPING, inplace=True)
                    
                    # 将修改后的数据写回原文件
                    df.to_parquet(file_path, index=False, compression='zstd')
                    files_processed += 1
                    logger.info(f"✅ 成功重命名并保存文件: {file_path}")

                except Exception as e:
                    logger.error(f"❌ 处理文件 {file_path} 时发生错误: {e}")
    
    logger.info(f"\n处理完成！总共更新了 {files_processed} 个 Parquet 文件。" )

if __name__ == "__main__":
    if not os.path.isdir(ETF_DATA_ROOT):
        logger.warning(f"目录 {ETF_DATA_ROOT} 不存在，无需执行重命名操作。" )
    else:
        rename_columns_in_parquet_files(ETF_DATA_ROOT)