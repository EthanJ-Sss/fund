"""
决策引擎 - 核心决策逻辑
"""
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple
from loguru import logger

from config.settings import settings
from ..models import (
    Portfolio, TradeSuggestion, FundScore, MarketSummary,
    SignalType, ConfidenceLevel, IndexValuation
)
from ..collector import fund_collector, valuation_collector, news_collector
from ..analyzer import ai_advisor
from .risk_control import RiskController


class DecisionEngine:
    """决策引擎"""
    
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.risk_controller = RiskController(portfolio)
        self.scoring_config = settings.ScoringWeights
    
    def run_daily_analysis(
        self,
        watch_list: List[str] = None
    ) -> Dict:
        """运行每日分析
        
        Args:
            watch_list: 关注的基金列表 [{code, name}]
        
        Returns:
            包含所有分析结果的字典
        """
        logger.info("开始每日分析...")
        
        result = {
            "date": date.today().isoformat(),
            "market_analysis": None,
            "portfolio_analysis": None,
            "suggestions": {
                "buy": [],
                "sell": [],
                "hold": [],
                "watch": []
            },
            "risk_warnings": [],
            "execution_summary": []
        }
        
        # 1. 获取市场数据
        logger.info("获取市场数据...")
        market_summary = news_collector.get_market_summary()
        valuations = valuation_collector.get_all_valuations()
        market_valuation = valuation_collector.get_market_overall_valuation()
        
        # 2. 检查市场异常
        anomaly = news_collector.check_market_anomaly()
        if anomaly["has_anomaly"]:
            result["risk_warnings"].append(anomaly["recommendation"])
            for a in anomaly["anomalies"]:
                result["risk_warnings"].append(f"{a['type']}: {a['value']*100:.2f}%")
        
        # 3. AI分析市场状况
        logger.info("AI分析市场状况...")
        market_analysis = ai_advisor.analyze_market_condition(
            market_summary,
            valuations,
            market_summary.news_highlights
        )
        result["market_analysis"] = market_analysis
        
        # 4. 分析现有持仓
        logger.info("分析现有持仓...")
        for position in self.portfolio.positions:
            suggestion = self._analyze_position(position, market_analysis, market_summary)
            
            # 分类建议
            if suggestion.signal == SignalType.SELL:
                result["suggestions"]["sell"].append(suggestion.model_dump())
            elif suggestion.signal == SignalType.HOLD:
                result["suggestions"]["hold"].append(suggestion.model_dump())
        
        # 5. 分析关注列表（潜在买入）
        if watch_list:
            logger.info("分析关注列表...")
            for fund_info in watch_list:
                code = fund_info.get("code") or fund_info
                name = fund_info.get("name", code) if isinstance(fund_info, dict) else code
                
                # 跳过已持有的
                if self.portfolio.get_position(code):
                    continue
                
                suggestion = self._analyze_fund_for_buy(
                    code, name, market_analysis, market_summary
                )
                
                if suggestion:
                    if suggestion.signal == SignalType.BUY:
                        result["suggestions"]["buy"].append(suggestion.model_dump())
                    else:
                        result["suggestions"]["watch"].append(suggestion.model_dump())
        
        # 6. 组合健康检查
        health = self.risk_controller.check_portfolio_health()
        result["portfolio_analysis"] = health
        
        if health["issues"]:
            result["risk_warnings"].extend(health["issues"])
        
        # 7. 生成执行摘要
        result["execution_summary"] = self._generate_execution_summary(result)
        
        logger.info("每日分析完成")
        return result
    
    def _analyze_position(
        self,
        position,
        market_analysis: Dict,
        market_summary: MarketSummary
    ) -> TradeSuggestion:
        """分析单个持仓"""
        
        # 获取最新净值
        nav_data = fund_collector.get_fund_estimate(position.fund_code)
        if nav_data:
            position.update_price(nav_data.nav)
        
        # 获取业绩数据
        performance = fund_collector.get_fund_performance(position.fund_code)
        
        # AI评分
        fund_score = ai_advisor.analyze_fund(
            position.fund_code,
            position.fund_name,
            performance or {},
            fund_collector.get_fund_detail(position.fund_code)
        )
        
        # 检查止盈止损条件
        take_profit_threshold = settings.RiskControl.TAKE_PROFIT_THRESHOLDS.get(
            position.fund_type.value, 0.20
        )
        
        # 止盈检查
        if position.profit_rate >= take_profit_threshold:
            return TradeSuggestion(
                fund_code=position.fund_code,
                fund_name=position.fund_name,
                signal=SignalType.SELL,
                confidence=ConfidenceLevel.HIGH,
                suggested_amount=position.shares * 0.5,  # 建议卖出一半
                reasons=[
                    f"收益率{position.profit_rate*100:.2f}%，达到{take_profit_threshold*100}%止盈线",
                    "建议分批止盈，锁定利润"
                ],
                risk_warnings=["市场可能继续上涨，可保留部分仓位"],
                score=fund_score
            )
        
        # 止损检查
        if position.profit_rate <= settings.RiskControl.STOP_LOSS_THRESHOLD:
            return TradeSuggestion(
                fund_code=position.fund_code,
                fund_name=position.fund_name,
                signal=SignalType.SELL,
                confidence=ConfidenceLevel.VERY_HIGH,
                suggested_amount=position.shares,  # 全部卖出
                reasons=[
                    f"亏损{position.profit_rate*100:.2f}%，触发{settings.RiskControl.STOP_LOSS_THRESHOLD*100}%止损线",
                    "严格执行止损纪律，控制风险"
                ],
                risk_warnings=["止损可能错过反弹，但保护本金更重要"],
                score=fund_score
            )
        
        # 生成AI建议
        suggestion = ai_advisor.generate_trade_suggestion(
            self.portfolio,
            position.fund_code,
            position.fund_name,
            fund_score,
            position.current_price,
            market_analysis
        )
        
        return suggestion
    
    def _analyze_fund_for_buy(
        self,
        fund_code: str,
        fund_name: str,
        market_analysis: Dict,
        market_summary: MarketSummary
    ) -> Optional[TradeSuggestion]:
        """分析是否应该买入某基金"""
        
        # 获取基金数据
        nav_data = fund_collector.get_fund_estimate(fund_code)
        if not nav_data:
            logger.warning(f"无法获取基金 {fund_code} 的净值数据")
            return None
        
        performance = fund_collector.get_fund_performance(fund_code)
        if not performance:
            logger.warning(f"无法获取基金 {fund_code} 的业绩数据")
            return None
        
        fund_detail = fund_collector.get_fund_detail(fund_code)
        
        # AI评分
        fund_score = ai_advisor.analyze_fund(
            fund_code,
            nav_data.name or fund_name,
            performance,
            fund_detail
        )
        
        # 检查评分是否达到买入标准
        if fund_score.total_score < self.scoring_config.BUY_THRESHOLD:
            return TradeSuggestion(
                fund_code=fund_code,
                fund_name=nav_data.name or fund_name,
                signal=SignalType.WATCH,
                confidence=ConfidenceLevel.LOW,
                reasons=[f"综合评分{fund_score.total_score:.1f}分，未达到{self.scoring_config.BUY_THRESHOLD}分买入标准"],
                risk_warnings=[],
                score=fund_score
            )
        
        # 计算建议买入金额
        market_pe = 50  # 默认
        hs300 = valuation_collector.get_hs300_valuation()
        if hs300:
            market_pe = hs300.pe_percentile
        
        max_amount = self.risk_controller.get_max_buy_amount(fund_code, market_pe)
        
        # 根据评分和市场状况确定买入比例
        score_ratio = min(1.0, (fund_score.total_score - 60) / 40)  # 60-100分映射到0-1
        market_ratio = 1.0 - (market_pe / 100)  # PE越低买越多
        
        suggested_amount = max_amount * score_ratio * market_ratio * 0.5  # 保守起见再减半
        suggested_amount = max(1000, min(suggested_amount, max_amount))  # 至少1000，不超过上限
        
        # 生成AI建议
        suggestion = ai_advisor.generate_trade_suggestion(
            self.portfolio,
            fund_code,
            nav_data.name or fund_name,
            fund_score,
            nav_data.nav,
            market_analysis
        )
        
        # 更新建议金额
        if suggestion.signal == SignalType.BUY:
            suggestion.suggested_amount = suggested_amount
        
        return suggestion
    
    def _generate_execution_summary(self, analysis_result: Dict) -> List[str]:
        """生成执行摘要"""
        summary = []
        
        # 市场状况
        market = analysis_result.get("market_analysis", {})
        summary.append(f"【市场概况】{market.get('market_trend', '未知')}，估值{market.get('valuation_level', '未知')}")
        
        # 建议数量统计
        suggestions = analysis_result.get("suggestions", {})
        buy_count = len(suggestions.get("buy", []))
        sell_count = len(suggestions.get("sell", []))
        hold_count = len(suggestions.get("hold", []))
        
        if sell_count > 0:
            summary.append(f"【卖出建议】{sell_count}只基金建议卖出")
        if buy_count > 0:
            summary.append(f"【买入建议】{buy_count}只基金值得关注")
        if hold_count > 0:
            summary.append(f"【持有建议】{hold_count}只基金继续持有")
        
        # 风险提示
        if analysis_result.get("risk_warnings"):
            summary.append(f"【风险提示】共{len(analysis_result['risk_warnings'])}条警告")
        
        return summary
    
    def get_quick_suggestion(
        self,
        fund_code: str,
        fund_name: str = ""
    ) -> Optional[TradeSuggestion]:
        """快速获取单只基金的建议"""
        
        # 获取市场数据
        market_summary = news_collector.get_market_summary()
        valuations = valuation_collector.get_all_valuations()
        
        market_analysis = ai_advisor.analyze_market_condition(
            market_summary, valuations
        )
        
        # 检查是否已持有
        position = self.portfolio.get_position(fund_code)
        
        if position:
            return self._analyze_position(position, market_analysis, market_summary)
        else:
            return self._analyze_fund_for_buy(
                fund_code, fund_name, market_analysis, market_summary
            )
    
    def validate_suggestion(
        self,
        suggestion: TradeSuggestion
    ) -> Tuple[bool, List[str], List[str]]:
        """验证建议是否可执行
        
        Returns:
            (is_valid, warnings, blockers)
        """
        market_summary = news_collector.get_market_summary()
        return self.risk_controller.check_all_risks(suggestion, market_summary)
