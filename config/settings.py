"""
系统配置文件
"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class Settings:
    """系统配置类"""
    
    # ==================== API配置 ====================
    # OpenAI API配置（用于大模型分析）
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4")
    
    # ==================== 数据源配置 ====================
    # 天天基金API
    EASTMONEY_FUND_API = "http://fundgz.1234567.com.cn/js/{fund_code}.js"
    EASTMONEY_FUND_DETAIL = "http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
    EASTMONEY_FUND_LIST = "http://fund.eastmoney.com/js/fundcode_search.js"
    
    # 指数估值数据
    INDEX_VALUATION_API = "https://danjuanfunds.com/djapi/index_eva/dj"
    
    # ==================== 风控配置 ====================
    class RiskControl:
        # 单只基金最大仓位（占总资产比例）
        MAX_SINGLE_FUND_POSITION = 0.20
        
        # 同类型基金最大仓位
        MAX_CATEGORY_POSITION = 0.40
        
        # 单次买入最大比例（占可用资金）
        MAX_SINGLE_BUY_RATIO = 0.30
        
        # 每日最大交易次数
        MAX_DAILY_TRADES = 3
        
        # 同一基金买入冷静期（天）
        BUY_COOLDOWN_DAYS = 7
        
        # 止损线
        STOP_LOSS_THRESHOLD = -0.15
        
        # 止盈线（不同类型基金）
        TAKE_PROFIT_THRESHOLDS = {
            "货币基金": 0.05,
            "债券基金": 0.10,
            "混合基金": 0.20,
            "股票基金": 0.25,
            "指数基金": 0.20,
        }
        
        # 熔断条件
        CIRCUIT_BREAKER = {
            "daily_loss": -0.03,      # 单日亏损3%暂停买入
            "weekly_loss": -0.05,     # 周亏损5%暂停交易
            "monthly_loss": -0.10,    # 月亏损10%人工介入
            "market_crash": -0.05,    # 市场单日跌5%暂停交易
        }
        
        # 估值分位对应的最大仓位
        VALUATION_POSITION_MAP = {
            20: 0.90,   # PE分位<20%，最高90%仓位
            40: 0.75,   # PE分位<40%，最高75%仓位
            60: 0.60,   # PE分位<60%，最高60%仓位
            80: 0.40,   # PE分位<80%，最高40%仓位
            100: 0.20,  # PE分位>80%，最高20%仓位
        }
    
    # ==================== 评分权重 ====================
    class ScoringWeights:
        # 买入评分权重
        FUND_QUALITY = 0.30      # 基金质量
        VALUATION = 0.30         # 估值
        TREND = 0.20             # 趋势
        RISK = 0.20              # 风险
        
        # 买入阈值
        BUY_THRESHOLD = 70
        
        # 置信度阈值
        CONFIDENCE_THRESHOLDS = {
            5: 90,   # ★★★★★ 综合评分>=90
            4: 80,   # ★★★★☆ 综合评分>=80
            3: 70,   # ★★★☆☆ 综合评分>=70
            2: 60,   # ★★☆☆☆ 综合评分>=60
            1: 0,    # ★☆☆☆☆ 综合评分<60
        }
    
    # ==================== 定时任务配置 ====================
    class Scheduler:
        # 任务时间表（24小时制）
        SCHEDULE = {
            "morning_news": "09:00",       # 早间新闻采集
            "market_open": "09:30",        # 开盘数据
            "midday_analysis": "11:30",    # 午间分析
            "market_close": "15:00",       # 收盘数据
            "nav_update": "16:00",         # 净值更新
            "daily_report": "18:00",       # 生成报告
            "send_notification": "20:00",  # 发送通知
        }
        
        # 时区
        TIMEZONE = "Asia/Shanghai"
    
    # ==================== 通知配置 ====================
    class Notification:
        # 邮件配置
        SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.qq.com")
        SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
        SMTP_USER = os.getenv("SMTP_USER", "")
        SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
        EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "")
        
        # 企业微信配置
        WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK", "")
        
        # 钉钉配置
        DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")
    
    # ==================== 数据存储路径 ====================
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    PORTFOLIO_FILE = os.path.join(DATA_DIR, "portfolio.json")
    TRADE_HISTORY_FILE = os.path.join(DATA_DIR, "trade_history.json")
    DAILY_REPORTS_DIR = os.path.join(DATA_DIR, "daily_reports")
    CACHE_DIR = os.path.join(DATA_DIR, "cache")
    
    # ==================== 日志配置 ====================
    LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# 创建配置实例
settings = Settings()
