"""
数据模型定义
"""
from datetime import datetime, date as Date
from typing import Optional, List
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class FundType(str, Enum):
    """基金类型"""
    MONEY = "货币基金"
    BOND = "债券基金"
    HYBRID = "混合基金"
    STOCK = "股票基金"
    INDEX = "指数基金"
    QDII = "QDII基金"
    OTHER = "其他"


class SignalType(str, Enum):
    """信号类型"""
    BUY = "买入"
    SELL = "卖出"
    HOLD = "持有"
    WATCH = "观望"


class ConfidenceLevel(int, Enum):
    """置信度等级"""
    VERY_HIGH = 5  # ★★★★★
    HIGH = 4       # ★★★★☆
    MEDIUM = 3     # ★★★☆☆
    LOW = 2        # ★★☆☆☆
    VERY_LOW = 1   # ★☆☆☆☆


class FundInfo(BaseModel):
    """基金基本信息"""
    code: str = Field(..., description="基金代码")
    name: str = Field(..., description="基金名称")
    fund_type: FundType = Field(..., description="基金类型")
    manager: Optional[str] = Field(None, description="基金经理")
    company: Optional[str] = Field(None, description="基金公司")
    size: Optional[float] = Field(None, description="基金规模(亿)")
    establish_date: Optional[Date] = Field(None, description="成立日期")
    management_fee: Optional[float] = Field(None, description="管理费率")
    custodian_fee: Optional[float] = Field(None, description="托管费率")


class FundNav(BaseModel):
    """基金净值数据"""
    code: str = Field(..., description="基金代码")
    name: str = Field(..., description="基金名称")
    nav: float = Field(..., description="单位净值")
    acc_nav: Optional[float] = Field(None, description="累计净值")
    nav_date: Date = Field(..., description="净值日期")
    daily_return: Optional[float] = Field(None, description="日涨跌幅")
    estimate_nav: Optional[float] = Field(None, description="估算净值")
    estimate_return: Optional[float] = Field(None, description="估算涨跌幅")


class Position(BaseModel):
    """持仓记录"""
    fund_code: str = Field(..., description="基金代码")
    fund_name: str = Field(..., description="基金名称")
    fund_type: FundType = Field(..., description="基金类型")
    shares: float = Field(..., description="持有份额")
    cost_price: float = Field(..., description="成本价")
    current_price: float = Field(0, description="当前净值")
    market_value: float = Field(0, description="市值")
    profit_loss: float = Field(0, description="盈亏金额")
    profit_rate: float = Field(0, description="收益率")
    buy_date: Date = Field(..., description="买入日期")
    hold_days: int = Field(0, description="持有天数")
    
    def update_price(self, current_price: float):
        """更新当前价格并计算收益"""
        self.current_price = current_price
        self.market_value = self.shares * current_price
        cost_value = self.shares * self.cost_price
        self.profit_loss = self.market_value - cost_value
        self.profit_rate = self.profit_loss / cost_value if cost_value > 0 else 0
        self.hold_days = (Date.today() - self.buy_date).days


class Portfolio(BaseModel):
    """投资组合"""
    total_value: float = Field(0, description="总资产")
    cash: float = Field(0, description="现金")
    positions: List[Position] = Field(default_factory=list, description="持仓列表")
    last_update: Optional[datetime] = Field(default_factory=datetime.now, description="最后更新时间")
    
    def get_position(self, fund_code: str) -> Optional[Position]:
        """获取指定基金的持仓"""
        for pos in self.positions:
            if pos.fund_code == fund_code:
                return pos
        return None
    
    def calculate_total(self):
        """计算总资产"""
        position_value = sum(p.market_value for p in self.positions)
        self.total_value = self.cash + position_value
        
    def get_position_ratio(self, fund_code: str) -> float:
        """获取某基金的仓位占比"""
        if self.total_value <= 0:
            return 0
        pos = self.get_position(fund_code)
        if pos:
            return pos.market_value / self.total_value
        return 0
    
    def get_category_ratio(self, fund_type: FundType) -> float:
        """获取某类基金的仓位占比"""
        if self.total_value <= 0:
            return 0
        category_value = sum(p.market_value for p in self.positions if p.fund_type == fund_type)
        return category_value / self.total_value
    
    def get_total_position_ratio(self) -> float:
        """获取总仓位占比（非现金部分）"""
        if self.total_value <= 0:
            return 0
        return 1 - (self.cash / self.total_value)


class TradeRecord(BaseModel):
    """交易记录"""
    id: str = Field(..., description="交易ID")
    fund_code: str = Field(..., description="基金代码")
    fund_name: str = Field(..., description="基金名称")
    trade_type: SignalType = Field(..., description="交易类型")
    shares: float = Field(..., description="交易份额")
    price: float = Field(..., description="交易价格")
    amount: float = Field(..., description="交易金额")
    fee: float = Field(0, description="手续费")
    trade_date: datetime = Field(default_factory=datetime.now, description="交易时间")
    reason: str = Field("", description="交易原因")
    confidence: ConfidenceLevel = Field(ConfidenceLevel.MEDIUM, description="置信度")


class IndexValuation(BaseModel):
    """指数估值数据"""
    index_code: str = Field(..., description="指数代码")
    index_name: str = Field(..., description="指数名称")
    pe: float = Field(..., description="市盈率")
    pe_percentile: float = Field(..., description="PE历史百分位")
    pb: float = Field(..., description="市净率")
    pb_percentile: float = Field(..., description="PB历史百分位")
    dividend_yield: Optional[float] = Field(None, description="股息率")
    update_date: Date = Field(..., description="更新日期")
    
    def get_valuation_level(self) -> str:
        """获取估值水平"""
        avg_percentile = (self.pe_percentile + self.pb_percentile) / 2
        if avg_percentile < 20:
            return "极度低估"
        elif avg_percentile < 40:
            return "低估"
        elif avg_percentile < 60:
            return "正常"
        elif avg_percentile < 80:
            return "高估"
        else:
            return "极度高估"


class FundScore(BaseModel):
    """基金评分"""
    fund_code: str = Field(..., description="基金代码")
    fund_name: str = Field(..., description="基金名称")
    quality_score: float = Field(0, description="质量评分(0-100)")
    valuation_score: float = Field(0, description="估值评分(0-100)")
    trend_score: float = Field(0, description="趋势评分(0-100)")
    risk_score: float = Field(0, description="风险评分(0-100)")
    total_score: float = Field(0, description="综合评分(0-100)")
    score_details: dict = Field(default_factory=dict, description="评分细节")
    
    def calculate_total(self, weights: dict = None):
        """计算综合评分"""
        if weights is None:
            weights = {
                "quality": 0.30,
                "valuation": 0.30,
                "trend": 0.20,
                "risk": 0.20,
            }
        self.total_score = (
            self.quality_score * weights["quality"] +
            self.valuation_score * weights["valuation"] +
            self.trend_score * weights["trend"] +
            self.risk_score * weights["risk"]
        )
        return self.total_score


class TradeSuggestion(BaseModel):
    """交易建议"""
    fund_code: str = Field(..., description="基金代码")
    fund_name: str = Field(..., description="基金名称")
    signal: SignalType = Field(..., description="信号类型")
    confidence: ConfidenceLevel = Field(..., description="置信度")
    suggested_amount: Optional[float] = Field(None, description="建议金额")
    suggested_ratio: Optional[float] = Field(None, description="建议比例")
    reasons: List[str] = Field(default_factory=list, description="理由列表")
    risk_warnings: List[str] = Field(default_factory=list, description="风险提示")
    score: Optional[FundScore] = Field(None, description="基金评分")
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    
    def get_confidence_stars(self) -> str:
        """获取置信度星级显示"""
        stars = "★" * self.confidence.value + "☆" * (5 - self.confidence.value)
        return stars


class MarketSummary(BaseModel):
    """市场摘要"""
    # pydantic v2: field name 'date' clashes with imported type 'date'
    model_config = ConfigDict(protected_namespaces=())

    date: Date = Field(..., description="日期")
    sh_index: float = Field(0, description="上证指数")
    sh_change: float = Field(0, description="上证涨跌幅")
    sz_index: float = Field(0, description="深证成指")
    sz_change: float = Field(0, description="深证涨跌幅")
    hs300_index: float = Field(0, description="沪深300")
    hs300_change: float = Field(0, description="沪深300涨跌幅")
    market_sentiment: str = Field("中性", description="市场情绪")
    hot_sectors: List[str] = Field(default_factory=list, description="热门板块")
    news_highlights: List[str] = Field(default_factory=list, description="要闻摘要")


class DailyReport(BaseModel):
    """每日报告"""
    report_date: Date = Field(..., description="报告日期")
    portfolio_summary: dict = Field(default_factory=dict, description="持仓概览")
    market_summary: Optional[MarketSummary] = Field(None, description="市场摘要")
    position_details: List[dict] = Field(default_factory=list, description="持仓明细")
    buy_suggestions: List[TradeSuggestion] = Field(default_factory=list, description="买入建议")
    sell_suggestions: List[TradeSuggestion] = Field(default_factory=list, description="卖出建议")
    hold_suggestions: List[TradeSuggestion] = Field(default_factory=list, description="持有建议")
    risk_warnings: List[str] = Field(default_factory=list, description="风险提示")
    created_at: datetime = Field(default_factory=datetime.now, description="生成时间")
