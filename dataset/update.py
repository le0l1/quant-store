#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化数据存储脚本 - 使用DuckDB
支持每日数据更新和增量同步
"""

import duckdb
import pandas as pd
import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import hashlib
import sqlite3

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

# 从环境变量读取Cookie
COOKIE = os.getenv("JISILU_COOKIE", "")

class QuantDataManager:
    """量化数据管理器"""
    
    def __init__(self, db_path: str = "quant_data.duckdb"):
        """
        初始化数据管理器
        
        Args:
            db_path: DuckDB数据库文件路径
        """
        self.db_path = db_path
        self.conn = None
        self.init_database()
    
    def init_database(self):
        """初始化数据库和表结构"""
        try:
            self.conn = duckdb.connect(self.db_path)
            
            # 创建可转债数据表 - 使用动态列结构
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS convertible_bonds (
                    id INTEGER PRIMARY KEY,
                    update_date DATE,
                    data_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建数据更新日志表
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS update_logs (
                    id INTEGER PRIMARY KEY,
                    table_name VARCHAR(50),
                    update_date DATE,
                    records_count INTEGER,
                    status VARCHAR(20),
                    error_message TEXT,
                    execution_time_ms INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建基础索引（只对已存在的列）
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cb_update_date ON convertible_bonds(update_date)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cb_data_hash ON convertible_bonds(data_hash)")
            
            logger.info("数据库初始化完成")
            
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def check_cookie(self) -> bool:
        """检查Cookie是否已设置"""
        if not COOKIE or COOKIE.strip() == "":
            logger.error("Cookie未设置! 请设置环境变量 JISILU_COOKIE")
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
            logger.info("正在请求集思录可转债数据...")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            logger.info(f"请求成功，状态码: {response.status_code}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return None
    
    def process_cb_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """处理API返回的数据"""
        try:
            if data.get('code') != 200:
                logger.error(f"API返回错误: {data.get('msg', '未知错误')}")
                return pd.DataFrame()
            
            cb_data = data.get('data', [])
            
            if not cb_data or not isinstance(cb_data, list):
                logger.error("数据格式不正确")
                return pd.DataFrame()
            
            df = pd.DataFrame(cb_data)
            
            # 显示API信息
            info = data.get('info', {})
            if info:
                logger.info(f"数据日期: {info.get('date', '未知')}")
            
            annual = data.get('annual', '')
            if annual:
                logger.info(f"年度: {annual}")
                
            return df
                
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            return pd.DataFrame()
    
    def process_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理原始数据，不做过滤，全部保留"""
        try:
            logger.info(f"原始数据量: {len(df)} 条")
            logger.info("保留所有数据，不进行过滤")
            return df
            
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            return pd.DataFrame()
    
    def calculate_data_hash(self, df: pd.DataFrame) -> str:
        """计算数据哈希值，用于检测数据变化"""
        # 选择关键字段进行哈希计算
        key_columns = ['bond_id', 'price', 'sprice', 'dblow', 'curr_iss_amt', 'premium_rt', 'increase_rt', 'bond_nm', 'stock_id']
        available_columns = [col for col in key_columns if col in df.columns]
        
        if not available_columns:
            return ""
        
        # 对数据进行排序后计算哈希
        try:
            sorted_data = df[available_columns].sort_values(available_columns).to_string(index=False)
            return hashlib.sha256(sorted_data.encode()).hexdigest()
        except Exception as e:
            logger.warning(f"计算数据哈希失败: {e}")
            # 如果排序失败，使用简单的字符串拼接
            data_str = df[available_columns].to_string(index=False)
            return hashlib.sha256(data_str.encode()).hexdigest()
    
    def update_convertible_bonds(self, force_update: bool = False) -> bool:
        """更新可转债数据"""
        start_time = datetime.now()
        
        try:
            # 检查今天是否已经更新过
            today = datetime.now().date()
            if not force_update:
                existing_count = self.conn.execute("""
                    SELECT COUNT(*) FROM convertible_bonds WHERE update_date = ?
                """, [today]).fetchone()[0]
                
                if existing_count > 0:
                    logger.info(f"今天({today})的数据已存在，跳过更新")
                    return True
            
            # 获取数据
            raw_data = self.fetch_cb_data()
            if raw_data is None:
                return False
            
            # 处理数据
            df = self.process_cb_data(raw_data)
            if df.empty:
                return False
            
            # 处理数据（不过滤，全部保留）
            processed_df = self.process_raw_data(df)
            if processed_df.empty:
                return False
            
            # 计算数据哈希
            data_hash = self.calculate_data_hash(processed_df)
            
            # 检查数据是否有变化
            if not force_update:
                latest_hash = self.conn.execute("""
                    SELECT data_hash FROM convertible_bonds 
                    ORDER BY update_date DESC, updated_at DESC 
                    LIMIT 1
                """).fetchone()
                
                if latest_hash and latest_hash[0] == data_hash:
                    logger.info("数据无变化，跳过更新")
                    return True
            
            # 准备数据
            processed_df['update_date'] = today
            processed_df['data_hash'] = data_hash
            processed_df['updated_at'] = datetime.now()
            
            # 删除今天的数据（如果存在）
            self.conn.execute("DELETE FROM convertible_bonds WHERE update_date = ?", [today])
            
            # 动态添加列（如果不存在）
            existing_columns = set()
            try:
                result = self.conn.execute("DESCRIBE convertible_bonds").fetchall()
                existing_columns = {row[0] for row in result}
            except:
                pass
            
            # 为每个新列添加列
            for col in processed_df.columns:
                if col not in existing_columns and col not in ['update_date', 'data_hash', 'updated_at']:
                    try:
                        # 尝试推断列类型
                        sample_value = processed_df[col].dropna().iloc[0] if not processed_df[col].dropna().empty else None
                        
                        # 根据数据类型和内容推断列类型
                        if pd.api.types.is_numeric_dtype(processed_df[col]):
                            if processed_df[col].dtype == 'int64':
                                col_type = "BIGINT"
                            else:
                                col_type = "DOUBLE"
                        elif isinstance(sample_value, bool):
                            col_type = "BOOLEAN"
                        elif isinstance(sample_value, list) or isinstance(sample_value, dict):
                            col_type = "JSON"
                        else:
                            col_type = "VARCHAR"
                        
                        self.conn.execute(f"ALTER TABLE convertible_bonds ADD COLUMN {col} {col_type}")
                        logger.info(f"添加新列: {col} ({col_type})")
                        
                        # 为重要列创建索引
                        if col in ['bond_id', 'bond_nm', 'stock_id']:
                            try:
                                self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cb_{col} ON convertible_bonds({col})")
                                logger.info(f"为列 {col} 创建索引")
                            except Exception as e:
                                logger.warning(f"创建索引失败 {col}: {e}")
                                
                    except Exception as e:
                        logger.warning(f"添加列 {col} 失败: {e}")
            
            # 准备插入数据
            processed_df['update_date'] = today
            processed_df['data_hash'] = data_hash
            processed_df['updated_at'] = datetime.now()
            
            # 处理JSON类型的数据
            for col in processed_df.columns:
                if col in ['icons', 't_flag'] and not processed_df[col].isna().all():
                    # 将列表/字典转换为JSON字符串
                    processed_df[col] = processed_df[col].apply(lambda x: json.dumps(x) if x is not None else None)
            
            # 获取下一个ID
            next_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM convertible_bonds").fetchone()[0]
            processed_df['id'] = range(next_id, next_id + len(processed_df))
            
            # 获取所有列名
            all_columns = [col for col in processed_df.columns]
            
            # 使用DuckDB的insert方法
            self.conn.execute(f"""
                INSERT INTO convertible_bonds ({', '.join(all_columns)})
                SELECT {', '.join(all_columns)} FROM processed_df
            """)
            
            # 记录更新日志
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            # 获取下一个ID
            next_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM update_logs").fetchone()[0]
            self.conn.execute("""
                INSERT INTO update_logs (id, table_name, update_date, records_count, status, execution_time_ms)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [next_id, 'convertible_bonds', today, len(processed_df), 'SUCCESS', int(execution_time)])
            
            self.conn.commit()
            
            logger.info(f"可转债数据更新成功，共 {len(processed_df)} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"更新可转债数据失败: {e}")
            
            # 记录错误日志
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            # 获取下一个ID
            next_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM update_logs").fetchone()[0]
            self.conn.execute("""
                INSERT INTO update_logs (id, table_name, update_date, records_count, status, error_message, execution_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [next_id, 'convertible_bonds', today, 0, 'ERROR', str(e), int(execution_time)])
            
            self.conn.commit()
            return False
    
    def get_latest_data(self, table_name: str = "convertible_bonds", limit: int = 20) -> pd.DataFrame:
        """获取最新数据"""
        try:
            query = f"""
                SELECT * FROM {table_name}
                WHERE update_date = (SELECT MAX(update_date) FROM {table_name})
                LIMIT {limit}
            """
            
            result = self.conn.execute(query).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"获取最新数据失败: {e}")
            return pd.DataFrame()
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """获取数据统计信息"""
        try:
            stats = {}
            
            # 获取各表记录数
            tables = ['convertible_bonds', 'update_logs']
            for table in tables:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[f"{table}_count"] = count
            
            # 获取最新更新日期
            latest_date = self.conn.execute("""
                SELECT MAX(update_date) FROM convertible_bonds
            """).fetchone()[0]
            stats['latest_update_date'] = latest_date
            
            # 获取更新成功率
            success_count = self.conn.execute("""
                SELECT COUNT(*) FROM update_logs WHERE status = 'SUCCESS'
            """).fetchone()[0]
            
            total_count = self.conn.execute("SELECT COUNT(*) FROM update_logs").fetchone()[0]
            stats['update_success_rate'] = success_count / total_count if total_count > 0 else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """清理旧数据（已禁用）"""
        logger.info("数据清理功能已禁用，保留所有历史数据")
        pass
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="量化数据存储系统")
    parser.add_argument("--force", action="store_true", help="强制更新数据（忽略重复检查）")
    parser.add_argument("--db-path", default="quant_data.duckdb", help="数据库文件路径")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("量化数据存储系统 - DuckDB版本")
    print("=" * 60)
    
    # 创建数据管理器
    data_manager = QuantDataManager(args.db_path)
    
    try:
        # 更新可转债数据
        print("\n开始更新可转债数据...")
        success = data_manager.update_convertible_bonds(force_update=args.force)
        
        if success:
            print("✅ 数据更新成功!")
        else:
            print("❌ 数据更新失败")
        
        # 显示简要统计信息
        stats = data_manager.get_data_statistics()
        print(f"\n数据统计: 总记录数 {stats.get('convertible_bonds_count', 0)}, 最新更新日期 {stats.get('latest_update_date', 'N/A')}")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"❌ 程序执行失败: {e}")
    
    finally:
        data_manager.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    main()
