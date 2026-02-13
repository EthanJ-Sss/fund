"""
定时任务调度器
用于自动化每日分析和通知
"""
import os
import sys
from datetime import datetime
from loguru import logger

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    logger.warning("APScheduler未安装，定时任务功能不可用")

from config.settings import settings
from main import InvestmentAdvisor

# 量化选基模块
from src.workflow.fund_analysis import FundAnalysisWorkflow
from src.storage.fund_storage import fund_storage


def setup_logging():
    """配置日志"""
    log_dir = settings.LOG_DIR
    os.makedirs(log_dir, exist_ok=True)
    
    # 配置loguru
    logger.add(
        os.path.join(log_dir, "scheduler_{time:YYYY-MM-DD}.log"),
        rotation="1 day",
        retention="30 days",
        level=settings.LOG_LEVEL
    )


def morning_collection():
    """早间数据采集（09:00）"""
    logger.info("执行早间数据采集...")
    try:
        advisor = InvestmentAdvisor()
        # 这里可以添加特定的早间采集逻辑
        logger.info("早间数据采集完成")
    except Exception as e:
        logger.error(f"早间数据采集失败: {e}")


def daily_analysis():
    """每日分析（10:00）- 通过邮件发送"""
    logger.info("执行每日分析...")
    try:
        advisor = InvestmentAdvisor()
        report = advisor.run_daily_analysis()
        
        # 通过邮件发送报告
        from src.notify import notifier
        result = notifier.send_email(
            subject=f"【理财日报】{__import__('datetime').date.today().strftime('%Y-%m-%d')}",
            content=report
        )
        
        if result:
            logger.info("每日分析完成，邮件已发送")
        else:
            logger.warning("每日分析完成，但邮件发送失败")
    except Exception as e:
        logger.error(f"每日分析失败: {e}")


def market_check():
    """盘中检查（11:30, 14:30）"""
    logger.info("执行盘中检查...")
    try:
        from src.collector import news_collector
        from src.notify import notifier, wecom_bot
        
        # 检查市场异常
        anomaly = news_collector.check_market_anomaly()
        if anomaly["has_anomaly"]:
            # 通过企业微信发送预警
            for a in anomaly["anomalies"]:
                wecom_bot.send_market_alert(
                    alert_type="crash" if a["value"] < 0 else "surge",
                    index_name=a["type"].replace("大跌", "").replace("大涨", ""),
                    change_pct=a["value"],
                    message=anomaly.get("recommendation", "")
                )
            
            # 其他渠道
            message = "\n".join([
                f"类型: {a['type']}, 幅度: {a['value']*100:.2f}%"
                for a in anomaly["anomalies"]
            ])
            notifier.send_alert("risk", f"市场异常预警\n\n{message}")
            logger.warning(f"市场异常: {anomaly}")
        
        logger.info("盘中检查完成")
    except Exception as e:
        logger.error(f"盘中检查失败: {e}")


def fund_screening():
    """量化选基分析（每周日 20:00）"""
    logger.info("执行量化选基分析...")
    try:
        workflow = FundAnalysisWorkflow()
        
        # 运行完整分析
        result = workflow.run_full_analysis(
            fund_types=['股票型', '混合型', '指数型', '债券型'],
            top_n=50,
            use_cache=False,  # 强制刷新
            save_results=True
        )
        
        logger.info(f"量化选基分析完成，耗时: {result.get('elapsed_seconds', 0):.1f} 秒")
        
        # 发送通知
        try:
            from src.notify import wecom_bot
            
            if wecom_bot.enabled:
                # 构建推荐摘要
                summary_lines = ["📊 本周基金筛选结果\n"]
                
                for fund_type, top_funds in result.get('top_funds', {}).items():
                    if top_funds:
                        summary_lines.append(f"\n【{fund_type}】TOP 5:")
                        for fund in top_funds[:5]:
                            name = fund.get('fund_name', '')[:10]
                            code = fund.get('fund_code', '')
                            score = fund.get('total_score', 0)
                            summary_lines.append(f"  {name}({code}) {score:.1f}分")
                
                summary = "\n".join(summary_lines)
                wecom_bot.send_text(summary)
                logger.info("量化选基结果已发送到企业微信")
        except Exception as e:
            logger.warning(f"发送通知失败: {e}")
        
    except Exception as e:
        logger.error(f"量化选基分析失败: {e}")


def daily_fund_update():
    """每日基金数据更新（收盘后 16:00）"""
    logger.info("执行每日基金数据更新...")
    try:
        # 清理过期缓存
        fund_storage.clear_cache(older_than_days=7)
        
        # 更新推荐基金的最新数据
        workflow = FundAnalysisWorkflow()
        
        # 只更新评分 TOP 基金的数据
        for fund_type in ['股票型', '混合型', '指数型', '债券型']:
            top_funds = fund_storage.get_top_funds(fund_type, top_n=20)
            if top_funds.empty:
                continue
            
            logger.info(f"更新 {fund_type} TOP 20 基金数据...")
            for fund_code in top_funds['fund_code'].tolist():
                try:
                    workflow._get_fund_factors(fund_code, use_cache=False)
                except Exception as e:
                    logger.debug(f"更新基金 {fund_code} 失败: {e}")
        
        logger.info("每日基金数据更新完成")
    except Exception as e:
        logger.error(f"每日基金数据更新失败: {e}")


def run_scheduler():
    """运行定时调度器"""
    if not HAS_SCHEDULER:
        logger.error("APScheduler未安装，无法运行定时任务")
        logger.info("请运行: pip install apscheduler")
        return
    
    setup_logging()
    
    scheduler = BlockingScheduler(timezone=settings.Scheduler.TIMEZONE)
    
    # 早间数据采集 - 每个交易日09:00
    scheduler.add_job(
        morning_collection,
        CronTrigger(
            day_of_week='mon-fri',
            hour=9,
            minute=0
        ),
        id='morning_collection',
        name='早间数据采集'
    )
    
    # 盘中检查 - 每个交易日11:30
    scheduler.add_job(
        market_check,
        CronTrigger(
            day_of_week='mon-fri',
            hour=11,
            minute=30
        ),
        id='midday_check',
        name='午间检查'
    )
    
    # 盘中检查 - 每个交易日14:30
    scheduler.add_job(
        market_check,
        CronTrigger(
            day_of_week='mon-fri',
            hour=14,
            minute=30
        ),
        id='afternoon_check',
        name='午后检查'
    )
    
    # 每日分析 - 每个交易日10:00 通过邮件发送
    scheduler.add_job(
        daily_analysis,
        CronTrigger(
            day_of_week='mon-fri',
            hour=10,
            minute=0
        ),
        id='daily_analysis',
        name='每日邮件报告'
    )
    
    # 每日基金数据更新 - 每个交易日16:00
    scheduler.add_job(
        daily_fund_update,
        CronTrigger(
            day_of_week='mon-fri',
            hour=16,
            minute=0
        ),
        id='daily_fund_update',
        name='每日基金数据更新'
    )
    
    # 量化选基分析 - 每周日20:00
    scheduler.add_job(
        fund_screening,
        CronTrigger(
            day_of_week='sun',
            hour=20,
            minute=0
        ),
        id='fund_screening',
        name='量化选基分析'
    )
    
    logger.info("="*60)
    logger.info("智能理财助手 - 定时任务调度器已启动")
    logger.info("="*60)
    logger.info("计划任务:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")
    logger.info("="*60)
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("调度器已停止")


def run_once():
    """立即执行一次分析（用于测试）"""
    setup_logging()
    logger.info("手动执行每日分析...")
    daily_analysis()


def send_test_email():
    """发送测试邮件"""
    setup_logging()
    logger.info("发送测试邮件...")
    
    try:
        from src.notify import notifier
        from config.settings import settings
        
        # 检查配置
        email_config = settings.Notification
        logger.info(f"SMTP服务器: {email_config.SMTP_SERVER}")
        logger.info(f"发件人: {email_config.SMTP_USER}")
        logger.info(f"收件人: {email_config.EMAIL_RECEIVER}")
        
        # 生成测试报告
        advisor = InvestmentAdvisor()
        report = advisor.run_daily_analysis()
        
        # 发送邮件
        result = notifier.send_email(
            subject=f"【理财日报-测试】{__import__('datetime').date.today().strftime('%Y-%m-%d')}",
            content=report
        )
        
        if result:
            logger.info("✅ 测试邮件发送成功！请检查收件箱: " + email_config.EMAIL_RECEIVER)
        else:
            logger.error("❌ 测试邮件发送失败，请检查配置")
            
    except Exception as e:
        logger.error(f"发送测试邮件失败: {e}")
        import traceback
        traceback.print_exc()


def run_fund_screening():
    """立即执行量化选基分析"""
    setup_logging()
    logger.info("手动执行量化选基分析...")
    fund_screening()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='智能理财助手定时任务')
    parser.add_argument(
        '--once', 
        action='store_true', 
        help='立即执行一次分析'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='执行一次盘中检查'
    )
    parser.add_argument(
        '--email',
        action='store_true',
        help='立即发送一封测试邮件'
    )
    parser.add_argument(
        '--screen',
        action='store_true',
        help='立即执行量化选基分析'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='立即更新基金数据'
    )
    
    args = parser.parse_args()
    
    if args.email:
        send_test_email()
    elif args.once:
        run_once()
    elif args.check:
        setup_logging()
        market_check()
    elif args.screen:
        run_fund_screening()
    elif args.update:
        setup_logging()
        daily_fund_update()
    else:
        run_scheduler()
