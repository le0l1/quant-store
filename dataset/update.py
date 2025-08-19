#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化数据更新脚本 - 使用分区Parquet文件存储
支持每日数据获取并按月合并到Parquet分区。
"""

import pandas as pd
import requests
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# --- 配置 ---
# Parquet文件的根目录
OUTPUT_DIR = 'data'
# 数据源的表名，将用作输出目录的一部分
TABLE_NAME = 'convertible_bonds'
# 用于分区的日期列
DATE_COLUMN = 'update_date'
# 用于唯一识别记录的列，在合并数据时去重
UNIQUE_COLUMNS = ['bond_id', 'update_date']

# 从环境变量读取Cookie
COOKIE = os.getenv("JISILU_COOKIE", "")
# --- 配置结束 ---

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_update.log'),
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
        logger.warning("输入的数据为空，无需保存。\n")
        return

    if DATE_COLUMN not in df.columns:
        logger.error(f"数据中缺少指定的日期列 '{DATE_COLUMN}'，无法进行分区保存。\n")
        return

    # 确保日期列是datetime类型
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])

    # 创建 'year_month' 分区列
    df['year_month'] = df[DATE_COLUMN].dt.strftime('%Y-%m')
    logger.info("已创建 'year_month' 分区列。\n")

    # --- 通用类型问题解决方案 ---
    # 遍历所有列，将 object 类型的列统一转换为字符串，以避免 Parquet 写入错误。
    # 这可以处理包含列表、字典等复杂对象的列。
    for col in df.columns:
        if df[col].dtype == 'object':
            # 对于list或dict类型，使用json.dumps进行序列化，其他类型直接转为字符串
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
            df[col] = df[col].astype(str) # 再次确保最终类型为string
            logger.info(f"已将 object 类型的列 '{col}' 统一转换为字符串以确保兼容性。")
    # --- 解决方案结束 ---

    # 按月份对新数据进行分组，以便逐月处理
    for year_month, group_df in df.groupby('year_month'):
        partition_path = os.path.join(output_dir, table_name, f"year_month={year_month}")
        os.makedirs(partition_path, exist_ok=True)
        
        # 在每个分区内，我们使用一个标准的文件名
        file_path = os.path.join(partition_path, 'data.parquet')
        
        logger.info(f"--- 正在处理分区: {partition_path} ---")

        try:
            # 如果该分区已存在数据文件，则执行合并操作
            if os.path.exists(file_path):
                logger.info(f"发现现有数据文件: {file_path}，开始执行合并操作。\n")
                existing_df = pd.read_parquet(file_path)
                
                # 合并新旧数据
                combined_df = pd.concat([existing_df, group_df], ignore_index=True)
                
                # 基于唯一键去重，保留最新的记录
                merged_df = combined_df.drop_duplicates(subset=UNIQUE_COLUMNS, keep='last')
                
                logger.info(f"合并完成: 旧记录数={len(existing_df)}, 新记录数={len(group_df)}, 合并后总数={len(merged_df)}\n")
                final_df = merged_df
            else:
                logger.info("未发现现有数据，将直接写入新数据。\n")
                final_df = group_df

            # 将最终的DataFrame写回Parquet文件，覆盖旧文件
            final_df.to_parquet(
                file_path,
                compression='zstd',
                index=False
            )
            logger.info(f"✅ 成功将 {len(final_df)} 条记录写入到: {file_path}\n")

        except Exception as e:
            logger.error(f"❌ 处理分区 {partition_path} 时发生错误: {e}\n")


class QuantDataManager:
    """量化数据管理器 - Parquet版本"""

    def check_cookie(self) -> bool:
        """检查Cookie是否已设置"""
        if not COOKIE or COOKIE.strip() == "":
            logger.error("Cookie未设置! 请设置环境变量 JISILU_COOKIE\n")
            return False
        return True

    def fetch_cb_data(self) -> Optional[Dict[str, Any]]:
        """从集思录API获取可转债数据"""
        if not self.check_cookie():
            return None
            
        url = "https://www.jisilu.cn/webapi/cb/list/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Columns': '1,70,2,3,5,6,11,12,14,15,16,29,30,32,34,44,46,47,50,52,53,54,56,57,58,59,60,62,63,67',
            'Init': '1',
            'Cookie': COOKIE,
            'Referer': 'https://www.jisilu.cn/',
        }
        
        try:
            logger.info("正在请求集思录可转债数据...\n")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(f"请求成功，状态码: {response.status_code}\n")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"请求失败: {e}\n")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}\n")
            return None

    def process_cb_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """处理API返回的数据"""
        try:
            if data.get('code') != 200:
                logger.error(f"API返回错误: {data.get('msg', '未知错误')}\n")
                return pd.DataFrame()
            
            cb_data = data.get('data', [])
            if not cb_data or not isinstance(cb_data, list):
                logger.error("API数据格式不正确\n")
                return pd.DataFrame()
            
            df = pd.DataFrame(cb_data)
            logger.info(f"从API获取到 {len(df)} 条原始记录。\n")
            return df
        except Exception as e:
            logger.error(f"数据处理失败: {e}\n")
            return pd.DataFrame()

    def update_convertible_bonds(self) -> bool:
        """获取、处理并保存可转债数据到Parquet文件"""
        start_time = datetime.now()
        
        try:
            # 1. 获取数据
            raw_data = self.fetch_cb_data()
            if raw_data is None:
                return False
            
            # 2. 处理数据
            df = self.process_cb_data(raw_data)
            if df.empty:
                return False
            
            # 3. 准备数据用于保存
            # 添加/覆盖更新日期列
            df[DATE_COLUMN] = datetime.now().date()
            
            # 4. 保存到分区的Parquet文件（包含合并逻辑）
            save_data_to_parquet(df, OUTPUT_DIR, TABLE_NAME)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"可转债数据更新成功，共处理 {len(df)} 条记录，耗时 {execution_time:.2f} 秒。\n")
            return True
            
        except Exception as e:
            logger.error(f"更新可转债数据失败: {e}\n")
            return False

def main():
    """主函数"""
    print("=" * 60)
    print("量化数据更新系统 - 分区Parquet版本")
    print("=" * 60)
    
    data_manager = QuantDataManager()
    
    try:
        print("\n开始更新可转债数据...")
        success = data_manager.update_convertible_bonds()
        
        if success:
            print("\n✅ 数据更新任务执行成功!")
        else:
            print("\n❌ 数据更新任务执行失败，请查看日志 data_update.log 获取详情。\n")
            
    except Exception as e:
        logger.error(f"程序主流程发生严重错误: {e}\n")
        print(f"❌ 程序执行失败: {e}\n")

if __name__ == "__main__":
    main()