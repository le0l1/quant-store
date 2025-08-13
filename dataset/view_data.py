#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查看DuckDB存储内容
"""

import duckdb
import os
import pandas as pd
from datetime import datetime

def view_database_content(db_path="quant_data.duckdb"):
    """查看数据库内容"""
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件 {db_path} 不存在")
        return
    
    try:
        conn = duckdb.connect(db_path)
        
        print("=" * 80)
        print("DuckDB 数据库内容查看")
        print("=" * 80)
        print(f"数据库文件: {db_path}")
        print(f"文件大小: {os.path.getsize(db_path) / 1024 / 1024:.2f} MB")
        print(f"查看时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # 获取所有表
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📋 数据库中的表: {[table[0] for table in tables]}")
        
        for table in tables:
            table_name = table[0]
            print(f"\n{'='*60}")
            print(f"📊 表: {table_name}")
            print(f"{'='*60}")
            
            # 获取表结构
            try:
                columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                print(f"📝 表结构 ({len(columns)} 列):")
                for i, col in enumerate(columns, 1):
                    print(f"  {i:2d}. {col[0]:<20} {col[1]}")
            except Exception as e:
                print(f"❌ 获取表结构失败: {e}")
                continue
            
            # 获取记录数
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"\n📈 记录数: {count:,}")
            except Exception as e:
                print(f"❌ 获取记录数失败: {e}")
                continue
            
            if count == 0:
                print("   (表为空)")
                continue
            

            
            # 显示数据样本
            try:
                if table_name == "convertible_bonds":
                    # 可转债数据表 - 显示最新数据
                    latest_date = conn.execute(f"""
                        SELECT MAX(update_date) FROM {table_name}
                    """).fetchone()[0]
                    
                    if latest_date:
                        print(f"\n📅 最新数据日期: {latest_date}")
                        
                        # 获取最新数据样本
                        sample_data = conn.execute(f"""
                            SELECT * FROM {table_name} 
                            WHERE update_date = '{latest_date}'
                            LIMIT 5
                        """).fetchdf()
                        
                        if not sample_data.empty:
                            print(f"\n📋 最新数据样本 (显示前5条):")
                            print("-" * 80)
                            
                            # 显示关键字段
                            key_columns = ['bond_id', 'bond_nm', 'price', 'sprice', 'dblow', 'curr_iss_amt', 'premium_rt']
                            available_columns = [col for col in key_columns if col in sample_data.columns]
                            
                            if available_columns:
                                print(sample_data[available_columns].to_string(index=False))
                            else:
                                print(sample_data.head().to_string(index=False))
                        
                        # 显示统计信息
                        print(f"\n📊 数据统计:")
                        if 'dblow' in sample_data.columns:
                            print(f"  双低值范围: {sample_data['dblow'].min():.2f} - {sample_data['dblow'].max():.2f}")
                        if 'curr_iss_amt' in sample_data.columns:
                            print(f"  剩余规模范围: {sample_data['curr_iss_amt'].min():.2f} - {sample_data['curr_iss_amt'].max():.2f}")
                        if 'premium_rt' in sample_data.columns:
                            print(f"  溢价率范围: {sample_data['premium_rt'].min():.2f}% - {sample_data['premium_rt'].max():.2f}%")
                
                elif table_name == "update_logs":
                    # 更新日志表 - 显示最近的更新记录
                    recent_logs = conn.execute(f"""
                        SELECT * FROM {table_name}
                        ORDER BY created_at DESC
                        LIMIT 10
                    """).fetchdf()
                    
                    if not recent_logs.empty:
                        print(f"\n📋 最近更新记录 (显示前10条):")
                        print("-" * 80)
                        print(recent_logs.to_string(index=False))
                    
                    # 显示更新统计
                    stats = conn.execute(f"""
                        SELECT 
                            COUNT(*) as total_updates,
                            COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as success_count,
                            COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as error_count,
                            AVG(execution_time_ms) as avg_execution_time
                        FROM {table_name}
                    """).fetchone()
                    
                    print(f"\n📊 更新统计:")
                    print(f"  总更新次数: {stats[0]}")
                    print(f"  成功次数: {stats[1]}")
                    print(f"  失败次数: {stats[2]}")
                    if stats[3]:
                        print(f"  平均执行时间: {stats[3]:.2f} ms")
                
                else:
                    # 其他表 - 显示前几条记录
                    sample_data = conn.execute(f"""
                        SELECT * FROM {table_name}
                        LIMIT 5
                    """).fetchdf()
                    
                    if not sample_data.empty:
                        print(f"\n📋 数据样本 (显示前5条):")
                        print("-" * 80)
                        print(sample_data.to_string(index=False))
            
            except Exception as e:
                print(f"❌ 获取数据样本失败: {e}")
        
        conn.close()
        print(f"\n{'='*80}")
        print("✅ 数据库内容查看完成")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"❌ 查看数据库失败: {e}")

def export_data_to_csv(db_path="quant_data.duckdb", output_dir="exports"):
    """导出数据到CSV文件"""
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件 {db_path} 不存在")
        return
    
    # 创建导出目录
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        conn = duckdb.connect(db_path)
        
        # 获取所有表
        tables = conn.execute("SHOW TABLES").fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # 导出数据
            data = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            if not data.empty:
                filename = os.path.join(output_dir, f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                data.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✅ 导出 {table_name}: {len(data)} 条记录 -> {filename}")
            else:
                print(f"⚠️  表 {table_name} 为空，跳过导出")
        
        conn.close()
        print(f"\n📁 数据已导出到 {output_dir} 目录")
        
    except Exception as e:
        print(f"❌ 导出数据失败: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="查看DuckDB数据库内容")
    parser.add_argument("--db-path", default="quant_data.duckdb", help="数据库文件路径")
    parser.add_argument("--export", action="store_true", help="导出数据到CSV")
    parser.add_argument("--output-dir", default="exports", help="导出目录")
    
    args = parser.parse_args()
    
    # 查看数据库内容
    view_database_content(args.db_path)
    
    # 如果需要导出
    if args.export:
        print("\n" + "="*80)
        print("开始导出数据...")
        print("="*80)
        export_data_to_csv(args.db_path, args.output_dir) 