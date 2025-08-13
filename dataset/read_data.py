#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从DuckDB读取数据到DataFrame的示例脚本
"""

import duckdb
import pandas as pd
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DuckDBReader:
    """DuckDB数据读取器"""
    
    def __init__(self, db_path='quant_data.db'):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        logger.info(f"✅ 连接到DuckDB数据库: {db_path}")
    
    def get_all_data(self, table_name='convertible_bonds', limit=None):
        """
        读取表中的所有数据
        
        Args:
            table_name: 表名
            limit: 限制返回行数，None表示全部
        
        Returns:
            pandas.DataFrame: 数据框
        """
        try:
            if limit:
                query = f"SELECT * FROM {table_name} LIMIT {limit}"
            else:
                query = f"SELECT * FROM {table_name}"
            
            df = self.conn.execute(query).df()
            logger.info(f"✅ 成功读取 {len(df)} 行数据从表 {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取数据失败: {e}")
            return pd.DataFrame()
    
    def get_latest_data(self, table_name='convertible_bonds', days=7):
        """
        读取最近几天的数据
        
        Args:
            table_name: 表名
            days: 最近天数
        
        Returns:
            pandas.DataFrame: 数据框
        """
        try:
            query = f"""
            SELECT * FROM {table_name} 
            WHERE update_date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY update_date DESC, id DESC
            """
            
            df = self.conn.execute(query).df()
            logger.info(f"✅ 成功读取最近 {days} 天的 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取最近数据失败: {e}")
            return pd.DataFrame()
    
    def get_data_by_date(self, table_name='convertible_bonds', date=None):
        """
        读取指定日期的数据
        
        Args:
            table_name: 表名
            date: 日期字符串 (YYYY-MM-DD)，None表示今天
        
        Returns:
            pandas.DataFrame: 数据框
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            query = f"""
            SELECT * FROM {table_name} 
            WHERE update_date = '{date}'
            ORDER BY id DESC
            """
            
            df = self.conn.execute(query).df()
            logger.info(f"✅ 成功读取 {date} 的 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取指定日期数据失败: {e}")
            return pd.DataFrame()
    
    def get_filtered_data(self, table_name='convertible_bonds', **filters):
        """
        根据条件过滤数据
        
        Args:
            table_name: 表名
            **filters: 过滤条件，如 bond_nm='可转债名称'
        
        Returns:
            pandas.DataFrame: 数据框
        """
        try:
            where_conditions = []
            for column, value in filters.items():
                if isinstance(value, str):
                    where_conditions.append(f"{column} = '{value}'")
                else:
                    where_conditions.append(f"{column} = {value}")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {where_clause}
            ORDER BY update_date DESC, id DESC
            """
            
            df = self.conn.execute(query).df()
            logger.info(f"✅ 成功读取过滤后的 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取过滤数据失败: {e}")
            return pd.DataFrame()
    
    def get_data_statistics(self, table_name='convertible_bonds'):
        """
        获取数据统计信息
        
        Args:
            table_name: 表名
        
        Returns:
            dict: 统计信息
        """
        try:
            # 总记录数
            total_query = f"SELECT COUNT(*) as total FROM {table_name}"
            total = self.conn.execute(total_query).fetchone()[0]
            
            # 最新更新日期
            latest_date_query = f"SELECT MAX(update_date) as latest_date FROM {table_name}"
            latest_date = self.conn.execute(latest_date_query).fetchone()[0]
            
            # 数据日期范围
            date_range_query = f"""
            SELECT MIN(update_date) as min_date, MAX(update_date) as max_date 
            FROM {table_name}
            """
            date_range = self.conn.execute(date_range_query).fetchone()
            
            # 每日记录数
            daily_count_query = f"""
            SELECT update_date, COUNT(*) as count 
            FROM {table_name} 
            GROUP BY update_date 
            ORDER BY update_date DESC
            """
            daily_counts = self.conn.execute(daily_count_query).df()
            
            stats = {
                'total_records': total,
                'latest_date': latest_date,
                'date_range': {
                    'min_date': date_range[0],
                    'max_date': date_range[1]
                },
                'daily_counts': daily_counts
            }
            
            logger.info(f"✅ 成功获取数据统计信息")
            return stats
            
        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {e}")
            return {}
    
    def execute_custom_query(self, query):
        """
        执行自定义SQL查询
        
        Args:
            query: SQL查询语句
        
        Returns:
            pandas.DataFrame: 查询结果
        """
        try:
            df = self.conn.execute(query).df()
            logger.info(f"✅ 成功执行自定义查询，返回 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"❌ 执行自定义查询失败: {e}")
            return pd.DataFrame()
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("✅ 数据库连接已关闭")


def main():
    """主函数 - 演示各种读取方法"""
    print("=" * 60)
    print("DuckDB数据读取示例")
    print("=" * 60)
    
    # 创建读取器
    reader = DuckDBReader()
    
    try:
        # 1. 获取数据统计信息
        print("\n📊 数据统计信息:")
        stats = reader.get_data_statistics()
        if stats:
            print(f"总记录数: {stats['total_records']}")
            print(f"最新日期: {stats['latest_date']}")
            print(f"日期范围: {stats['date_range']['min_date']} 到 {stats['date_range']['max_date']}")
            
            if not stats['daily_counts'].empty:
                print("\n每日记录数:")
                print(stats['daily_counts'].head())
        
        # 2. 读取最近7天的数据
        print("\n📅 最近7天的数据:")
        recent_df = reader.get_latest_data(days=7)
        if not recent_df.empty:
            print(f"数据形状: {recent_df.shape}")
            print(f"列名: {list(recent_df.columns)}")
            print("\n前5行数据:")
            print(recent_df.head())
        
        # 3. 读取今天的数据
        print("\n📅 今天的数据:")
        today_df = reader.get_data_by_date()
        if not today_df.empty:
            print(f"今天有 {len(today_df)} 条记录")
            print(today_df.head())
        
        # 4. 自定义查询示例
        print("\n🔍 自定义查询示例:")
        custom_query = """
        SELECT 
            bond_nm,
            price,
            sprice,
            update_date,
            COUNT(*) as record_count
        FROM convertible_bonds 
        WHERE update_date >= CURRENT_DATE - INTERVAL '3' DAY
        GROUP BY bond_nm, price, sprice, update_date
        ORDER BY update_date DESC, price DESC
        LIMIT 10
        """
        custom_df = reader.execute_custom_query(custom_query)
        if not custom_df.empty:
            print(custom_df)
        
        # 5. 过滤数据示例
        print("\n🔍 过滤数据示例 (价格大于100的可转债):")
        filtered_df = reader.get_filtered_data(price__gt=100)
        if not filtered_df.empty:
            print(f"找到 {len(filtered_df)} 条价格大于100的记录")
            print(filtered_df[['bond_nm', 'price', 'update_date']].head())
    
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    
    finally:
        # 关闭连接
        reader.close()
        print("\n✅ 程序执行完成")


if __name__ == "__main__":
    main() 