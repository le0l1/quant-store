#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集思录可转债数据爬取脚本
接口地址: https://www.jisilu.cn/webapi/cb/list/
"""

import requests
import pandas as pd
import json
import os
from typing import Dict, Any, Optional

# 从环境变量读取Cookie，更安全，避免提交泄漏
COOKIE = os.getenv("JISILU_COOKIE", "")


def check_cookie() -> bool:
    """
    检查Cookie是否已设置
    """
    if not COOKIE or COOKIE.strip() == "":
        print("❌ 错误: Cookie未设置!")
        print("\n推荐做法: 通过环境变量设置，避免提交泄漏")
        print("export JISILU_COOKIE='你的Cookie字符串'")
        print("或在运行时临时设置: JISILU_COOKIE='你的Cookie' python double-low.py")
        print("\n获取Cookie步骤:")
        print("1. 打开浏览器，访问 https://www.jisilu.cn/")
        print("2. 登录你的账户")
        print("3. 按F12打开开发者工具")
        print("4. 切换到Network标签页")
        print("5. 刷新页面，找到对 www.jisilu.cn 的请求")
        print("6. 在请求头中找到Cookie字段并复制")
        return False
    return True


def fetch_cb_data() -> Optional[Dict[str, Any]]:
    """
    从集思录API获取可转债数据
    
    Returns:
        Dict: API返回的JSON数据，如果请求失败返回None
    """
    if not check_cookie():
        return None
        
    url = "https://www.jisilu.cn/webapi/cb/list/"
    
    # 设置请求头，模拟浏览器访问
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
        print("正在请求集思录可转债数据...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查HTTP状态码
        
        print(f"请求成功，状态码: {response.status_code}")
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析失败: {e}")
        return None


def process_cb_data(data: Dict[str, Any]) -> pd.DataFrame:
    """
    处理API返回的数据，转换为DataFrame
    
    Args:
        data: API返回的JSON数据
        
    Returns:
        pd.DataFrame: 处理后的可转债数据
    """
    try:
        # 检查API响应状态
        if data.get('code') != 200:
            print(f"API返回错误: {data.get('msg', '未知错误')}")
            return pd.DataFrame()
        
        # 获取可转债数据
        cb_data = data.get('data', [])

        if not cb_data:
            print("未找到可转债数据")
            return pd.DataFrame()
        
        if not isinstance(cb_data, list):
            print("数据格式不正确，期望列表格式")
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(cb_data)
        
        # 显示API信息
        info = data.get('info', {})
        if info:
            print(f"数据日期: {info.get('date', '未知')}")
        
        annual = data.get('annual', '')
        if annual:
            print(f"年度: {annual}")
            
        return df
            
    except Exception as e:
        print(f"数据处理失败: {e}")
        return pd.DataFrame()


def filter_and_sort_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    根据要求过滤和排序数据
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pd.DataFrame: 过滤和排序后的数据
    """
    try:
        print(f"\n原始数据量: {len(df)} 条")
        
        # 首先显示所有列名，帮助调试
        print(f"数据列名: {list(df.columns)}")
        
        # 检查必需的字段是否存在
        required_fields = ['bond_nm', 'price_tips', 'icons', 'sprice', 'dblow', 'curr_iss_amt', 'rating_cd']
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            print(f"缺少必需字段: {missing_fields}")
            return pd.DataFrame()
        
        # 1. 剔除 bond_nm 包含 '退' 字的行
        df_filtered = df[~df['bond_nm'].str.contains('退', na=False)]
        print(f"剔除包含'退'字的可转债后: {len(df_filtered)} 条")
        
        # 2. 剔除 "price_tips": "待上市"
        df_filtered = df_filtered[~df_filtered['price_tips'].str.contains('待上市', na=False)]
        print(f"剔除待上市的可转债后: {len(df_filtered)} 条")
        
        # 3. 剔除 "icons"下有R的 和 有O的
        def filter_icons(icons):
            if isinstance(icons, dict):
                return not ('R' in icons or 'O' in icons)
            elif isinstance(icons, list):
                return len(icons) == 0
            else:
                return True
        
        df_filtered = df_filtered[df_filtered['icons'].apply(filter_icons)]
        print(f"剔除icons包含R或O的可转债后: {len(df_filtered)} 条")
        
        # 4. sprice > 3
        df_filtered = df_filtered[df_filtered['sprice'] > 3]
        print(f"过滤sprice > 3后: {len(df_filtered)} 条")
        
        # 5. 剔除 rating_cd 不包含 'A' 的
        df_filtered = df_filtered[df_filtered['rating_cd'].astype(str).str.contains('A', case=False, na=False)]
        print(f"剔除 rating_cd 不包含 'A' 的后: {len(df_filtered)} 条")
        
        # 6. 计算排序指标: rank_indicator = dblow + curr_iss_amt
        df_filtered['rank_indicator'] = df_filtered['dblow'] + df_filtered['curr_iss_amt']
        print(f"计算排序指标: rank_indicator = dblow + curr_iss_amt")
        
        # 7. 根据 rank_indicator 进行升序排序，输出最小的 top 20
        df_sorted = df_filtered.sort_values('rank_indicator', ascending=True)
        result_df = df_sorted.head(20)
        
        print(f"最终筛选结果: {len(result_df)} 条")
        
        return result_df
        
    except Exception as e:
        print(f"数据过滤和排序失败: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
        return pd.DataFrame()


def main():
    """
    主函数
    """
    print("=" * 50)
    print("集思录可转债数据爬取工具")
    print("=" * 50)
    
    # 获取数据
    raw_data = fetch_cb_data()
    
    if raw_data is None:
        print("无法获取数据，程序退出")
        return
    
    # 处理数据
    print("正在处理数据...")
    df = process_cb_data(raw_data)
    
    if df.empty:
        print("处理后数据为空")
        return
    
    # 显示原始数据信息
    print(f"\n原始数据获取成功！共获取 {len(df)} 条可转债记录")
    print(f"数据列数: {len(df.columns)}")
    
    # 根据要求过滤和排序数据
    print("\n" + "="*50)
    print("根据要求进行数据过滤和排序:")
    print("1. 剔除 bond_nm 包含 '退' 字的行")
    print("2. 剔除 'price_tips': '待上市'")
    print("3. 剔除 'icons'下有R的 和 有O的")
    print("4. sprice > 3")
    print("5. 剔除 rating_cd 不包含 'A' 的")
    print("6. 计算排序指标: rank_indicator = dblow + curr_iss_amt")
    print("7. 根据 rank_indicator 进行升序排序，输出最小的 top 20")
    print("="*50)
    
    filtered_df = filter_and_sort_data(df)
    
    if filtered_df.empty:
        print("过滤后数据为空")
        return
    
    # 显示过滤后的数据
    print(f"\n" + "="*50)
    print("最终筛选结果:")
    print("="*50)
    
    # 设置pandas显示选项
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 15)
    
    # 显示关键字段
    key_columns = ['bond_id', 'bond_nm', 'price', 'sprice', 'dblow', 'curr_iss_amt', 'rank_indicator', 'premium_rt', 'increase_rt']
    available_columns = [col for col in key_columns if col in filtered_df.columns]
    
    print(f"\n筛选结果 (显示字段: {', '.join(available_columns)}):")
    print(filtered_df[available_columns].to_string(index=False))
    
    # 显示统计信息
    print(f"\n筛选结果统计:")
    if 'dblow' in filtered_df.columns:
        print(f"dblow范围: {filtered_df['dblow'].min():.2f} - {filtered_df['dblow'].max():.2f}")
    if 'curr_iss_amt' in filtered_df.columns:
        print(f"curr_iss_amt范围: {filtered_df['curr_iss_amt'].min():.2f} - {filtered_df['curr_iss_amt'].max():.2f}")
    if 'rank_indicator' in filtered_df.columns:
        print(f"rank_indicator范围: {filtered_df['rank_indicator'].min():.2f} - {filtered_df['rank_indicator'].max():.2f}")
    if 'sprice' in filtered_df.columns:
        print(f"sprice范围: {filtered_df['sprice'].min():.2f} - {filtered_df['sprice'].max():.2f}")
    if 'price' in filtered_df.columns:
        print(f"price范围: {filtered_df['price'].min():.2f} - {filtered_df['price'].max():.2f}")
    
    # 保存到CSV文件
    try:
        filename = "cb_filtered_data.csv"
        filtered_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n筛选结果已保存到 {filename}")
    except Exception as e:
        print(f"保存文件失败: {e}")


if __name__ == "__main__":
    main() 