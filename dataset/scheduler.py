#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据更新调度器
支持定时自动更新数据
"""

import schedule
import time
import logging
import os
from datetime import datetime
from update import QuantDataManager
from update_etf import update_etf_data

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def daily_update_job():
    """每日可转债数据更新任务"""
    logger.info("--- 开始执行每日可转债数据更新任务 ---")
    try:
        # QuantDataManager is now just a logical wrapper for the update process
        data_manager = QuantDataManager()
        success = data_manager.update_convertible_bonds()
        
        if success:
            logger.info("--- 每日可转债数据更新任务调度成功 ---")
        else:
            logger.error("--- 每日可转债数据更新任务执行失败 ---")
            
    except Exception as e:
        logger.error(f"每日可转债更新任务执行期间发生未捕获的异常: {e}")


def daily_update_etf_job():
    """每日ETF数据更新任务"""
    logger.info("--- 开始执行每日ETF数据更新任务 ---")
    try:
        # The update_etf_data function now handles its own logging internally
        update_etf_data()
        logger.info("--- 每日ETF数据更新任务调度成功 ---")
        # Note: Success/failure is logged within update_etf_data itself.
        # This log indicates the scheduler successfully completed the call.
    except Exception as e:
        logger.error(f"每日ETF更新任务执行期间发生未捕获的异常: {e}")


def setup_schedule():
    """设置定时任务"""
    # 设置时区为东八区（北京时间）
    os.environ['TZ'] = 'Asia/Shanghai'
    try:
        time.tzset()
        logger.info(f"✅ 时区设置成功: {time.tzname}")
    except AttributeError:
        logger.info("⚠️  tzset() 不可用，使用环境变量设置时区")
    
    # 每个工作日15:50更新数据（北京时间）
    schedule.every().monday.at("15:30").do(daily_update_job)
    schedule.every().tuesday.at("15:30").do(daily_update_job)
    schedule.every().wednesday.at("15:30").do(daily_update_job)
    schedule.every().thursday.at("15:30").do(daily_update_job)
    schedule.every().friday.at("15:30").do(daily_update_job)

    # 每个工作日17:35更新ETF数据（北京时间）
    schedule.every().monday.at("15:35").do(daily_update_etf_job)
    schedule.every().tuesday.at("15:35").do(daily_update_etf_job)
    schedule.every().wednesday.at("15:35").do(daily_update_etf_job)
    schedule.every().thursday.at("15:35").do(daily_update_etf_job)
    schedule.every().friday.at("15:35").do(daily_update_etf_job)
    
    # 显示当前时区信息
    current_time = datetime.now()
    logger.info(f"当前时间: {current_time}")
    logger.info(f"当前时区: {time.tzname}")
    logger.info("定时任务设置完成")
    logger.info("工作日 15:30 (北京时间) - 更新可转债数据")
    logger.info("工作日 15:35 (北京时间) - 更新ETF数据")


def run_scheduler():
    """运行调度器"""
    print("=" * 60)
    print("量化数据自动更新调度器")
    print("=" * 60)
    print("按 Ctrl+C 停止调度器")
    print("=" * 60)
    
    setup_schedule()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
            
    except KeyboardInterrupt:
        print("\n调度器已停止")
        logger.info("调度器被用户停止")


if __name__ == "__main__":
    run_scheduler() 