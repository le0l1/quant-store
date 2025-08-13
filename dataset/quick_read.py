#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速从DuckDB读取数据到DataFrame的简单示例
"""

import duckdb
import pandas as pd

def quick_read_example():
    """快速读取示例"""
    
    # 1. 连接数据库
    conn = duckdb.connect('quant_data.db')
    
    # 2. 基本读取 - 所有数据
    print("📊 读取所有数据:")
    df_all = conn.execute("SELECT * FROM convertible_bonds").df()
    print(f"总记录数: {len(df_all)}")
    print(f"列名: {list(df_all.columns)}")
    print(df_all.head())
    
    # 3. 读取最近数据
    print("\n📅 读取最近3天的数据:")
    df_recent = conn.execute("""
        SELECT * FROM convertible_bonds 
        WHERE update_date >= CURRENT_DATE - INTERVAL '3' DAY
        ORDER BY update_date DESC
    """).df()
    print(f"最近3天记录数: {len(df_recent)}")
    print(df_recent.head())
    
    # 4. 读取今天的数据
    print("\n📅 读取今天的数据:")
    df_today = conn.execute("""
        SELECT * FROM convertible_bonds 
        WHERE update_date = CURRENT_DATE
        ORDER BY id DESC
    """).df()
    print(f"今天记录数: {len(df_today)}")
    if not df_today.empty:
        print(df_today.head())
    
    # 5. 统计查询
    print("\n📈 数据统计:")
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            MIN(update_date) as min_date,
            MAX(update_date) as max_date,
            COUNT(DISTINCT update_date) as unique_dates
        FROM convertible_bonds
    """).df()
    print(stats)
    
    # 6. 分组统计
    print("\n📊 每日记录数统计:")
    daily_stats = conn.execute("""
        SELECT 
            update_date,
            COUNT(*) as record_count
        FROM convertible_bonds 
        GROUP BY update_date 
        ORDER BY update_date DESC
        LIMIT 10
    """).df()
    print(daily_stats)
    
    # 7. 条件过滤
    print("\n🔍 价格大于100的可转债:")
    high_price = conn.execute("""
        SELECT bond_nm, price, sprice, update_date
        FROM convertible_bonds 
        WHERE price > 100
        ORDER BY price DESC
        LIMIT 10
    """).df()
    print(high_price)
    
    # 8. 关闭连接
    conn.close()
    print("\n✅ 读取完成!")


if __name__ == "__main__":
    quick_read_example() 