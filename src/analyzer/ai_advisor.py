"""
AI顾问模块 - 使用大模型进行分析和决策
"""
import json
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from loguru import logger

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from config.settings import settings
from ..models import (
    Portfolio, Position, TradeSuggestion, FundScore, 
    SignalType, ConfidenceLevel, MarketSummary, IndexValuation
)


class AIAdvisor:
    """AI投资顾问"""
    
    def __init__(self):
        if OpenAI and settings.OPENAI_API_KEY:
            self.client = OpenAI(
                api_key=settings.OPENAI_API_KEY,
                base_url=settings.OPENAI_BASE_URL
            )
            self.model = settings.OPENAI_MODEL
            self.enabled = True
        else:
            self.client = None
            self.enabled = False
            logger.warning("OpenAI API未配置，AI分析功能将受限")
    
    def _call_llm(self, system_prompt: str, user_prompt: str) -> Optional[str]:
        """调用大模型"""
        if not self.enabled:
            logger.warning("AI功能未启用")
            return None
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,  # 使用较低温度确保稳定性
                max_tokens=2000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"调用AI失败: {e}")
            return None
    
    def analyze_market_condition(
        self,
        market_summary: MarketSummary,
        valuations: List[IndexValuation],
        news_highlights: List[str] = None
    ) -> Dict[str, Any]:
        """分析市场状况"""
        
        system_prompt = """你是一位专业的投资分析师，需要根据市场数据给出客观、谨慎的分析。
你的分析将影响真实的投资决策，因此必须：
1. 保持客观中立，不过度乐观或悲观
2. 明确指出风险因素
3. 给出具体的操作建议
4. 使用JSON格式输出"""

        valuation_info = "\n".join([
            f"- {v.index_name}: PE={v.pe:.2f}(历史{v.pe_percentile:.1f}%分位), PB={v.pb:.2f}(历史{v.pb_percentile:.1f}%分位)"
            for v in valuations
        ])
        
        news_info = "\n".join([f"- {n}" for n in (news_highlights or [])]) or "暂无要闻"
        
        user_prompt = f"""请分析以下市场数据：

【今日行情】
- 上证指数: {market_summary.sh_index:.2f} ({market_summary.sh_change*100:+.2f}%)
- 深证成指: {market_summary.sz_index:.2f} ({market_summary.sz_change*100:+.2f}%)
- 沪深300: {market_summary.hs300_index:.2f} ({market_summary.hs300_change*100:+.2f}%)
- 市场情绪: {market_summary.market_sentiment}

【指数估值】
{valuation_info}

【市场要闻】
{news_info}

请以JSON格式返回分析结果：
{{
    "market_trend": "上涨/震荡/下跌",
    "valuation_level": "低估/正常/高估",
    "risk_level": "低/中/高",
    "key_observations": ["观点1", "观点2"],
    "operation_suggestion": "具体建议",
    "max_position_ratio": 0.6,  // 建议的最大仓位比例
    "sectors_to_watch": ["板块1", "板块2"],
    "risk_warnings": ["风险1", "风险2"]
}}"""

        result = self._call_llm(system_prompt, user_prompt)
        
        if result:
            try:
                # 提取JSON部分
                json_start = result.find('{')
                json_end = result.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    return json.loads(result[json_start:json_end])
            except json.JSONDecodeError as e:
                logger.error(f"解析AI返回结果失败: {e}")
        
        # 返回默认分析
        return self._default_market_analysis(market_summary, valuations)
    
    def analyze_fund(
        self,
        fund_code: str,
        fund_name: str,
        performance: Dict[str, float],
        fund_detail: Dict = None,
        related_valuation: IndexValuation = None
    ) -> FundScore:
        """分析单只基金并评分"""
        
        system_prompt = """你是一位专业的基金分析师，需要对基金进行评分。
评分标准：
1. 质量评分(0-100)：基于长期业绩、基金经理、规模等
2. 估值评分(0-100)：基于跟踪指数的估值水平
3. 趋势评分(0-100)：基于近期表现和市场趋势
4. 风险评分(0-100)：基于波动率、回撤等（分数越高风险越可控）

请给出谨慎、客观的评分，避免过度乐观。"""

        perf_info = "\n".join([
            f"- {k}: {v*100:.2f}%" if v else f"- {k}: 无数据"
            for k, v in performance.items()
        ])
        
        val_info = ""
        if related_valuation:
            val_info = f"""
【相关指数估值】
- {related_valuation.index_name}: PE百分位{related_valuation.pe_percentile:.1f}%
"""
        
        manager_info = ""
        if fund_detail and fund_detail.get("managers"):
            managers = fund_detail["managers"]
            manager_info = f"- 基金经理: {managers[0].get('name', '未知')}, 任职{managers[0].get('work_days', '未知')}天"
        
        user_prompt = f"""请分析以下基金：

【基金信息】
- 代码: {fund_code}
- 名称: {fund_name}
{manager_info}

【业绩表现】
{perf_info}
{val_info}

请以JSON格式返回评分：
{{
    "quality_score": 70,
    "valuation_score": 65,
    "trend_score": 60,
    "risk_score": 75,
    "score_details": {{
        "quality_reason": "评分理由",
        "valuation_reason": "评分理由",
        "trend_reason": "评分理由",
        "risk_reason": "评分理由"
    }},
    "summary": "总体评价"
}}"""

        result = self._call_llm(system_prompt, user_prompt)
        
        score = FundScore(
            fund_code=fund_code,
            fund_name=fund_name
        )
        
        if result:
            try:
                json_start = result.find('{')
                json_end = result.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    data = json.loads(result[json_start:json_end])
                    score.quality_score = data.get("quality_score", 50)
                    score.valuation_score = data.get("valuation_score", 50)
                    score.trend_score = data.get("trend_score", 50)
                    score.risk_score = data.get("risk_score", 50)
                    score.score_details = data.get("score_details", {})
                    score.calculate_total()
                    return score
            except Exception as e:
                logger.error(f"解析基金评分失败: {e}")
        
        # 使用默认评分逻辑
        return self._default_fund_score(fund_code, fund_name, performance)
    
    def generate_trade_suggestion(
        self,
        portfolio: Portfolio,
        fund_code: str,
        fund_name: str,
        fund_score: FundScore,
        current_price: float,
        market_analysis: Dict[str, Any]
    ) -> TradeSuggestion:
        """生成交易建议"""
        
        system_prompt = """你是一位谨慎的投资顾问，需要根据分析结果给出买入/卖出/持有建议。
原则：
1. 宁可错过机会，不可冒险亏损
2. 必须考虑仓位限制和风险控制
3. 每个建议都要有充分理由
4. 明确标注置信度(1-5星)

置信度标准：
5星：强烈建议，多个有利信号共振
4星：建议执行，条件较好
3星：可以考虑，但需谨慎
2星：信号不强，建议观望
1星：不建议操作"""

        # 获取当前持仓
        position = portfolio.get_position(fund_code)
        position_info = ""
        if position:
            position_info = f"""
【当前持仓】
- 持有份额: {position.shares}
- 成本价: {position.cost_price:.4f}
- 当前价: {position.current_price:.4f}
- 收益率: {position.profit_rate*100:.2f}%
- 持有天数: {position.hold_days}天
- 仓位占比: {portfolio.get_position_ratio(fund_code)*100:.2f}%
"""
        else:
            position_info = "【当前持仓】未持有该基金"
        
        user_prompt = f"""请为以下基金生成交易建议：

【基金信息】
- 代码: {fund_code}
- 名称: {fund_name}
- 当前净值: {current_price:.4f}

【基金评分】
- 质量评分: {fund_score.quality_score:.1f}
- 估值评分: {fund_score.valuation_score:.1f}
- 趋势评分: {fund_score.trend_score:.1f}
- 风险评分: {fund_score.risk_score:.1f}
- 综合评分: {fund_score.total_score:.1f}

{position_info}

【市场状况】
- 市场趋势: {market_analysis.get('market_trend', '未知')}
- 估值水平: {market_analysis.get('valuation_level', '未知')}
- 风险等级: {market_analysis.get('risk_level', '未知')}
- 建议最大仓位: {market_analysis.get('max_position_ratio', 0.6)*100:.0f}%

【账户状态】
- 总资产: ¥{portfolio.total_value:,.2f}
- 可用现金: ¥{portfolio.cash:,.2f}
- 当前总仓位: {portfolio.get_total_position_ratio()*100:.1f}%

请以JSON格式返回交易建议：
{{
    "signal": "买入/卖出/持有/观望",
    "confidence": 4,
    "suggested_amount": 5000,  // 建议金额(买入)或份额(卖出)
    "reasons": ["理由1", "理由2"],
    "risk_warnings": ["风险1", "风险2"],
    "execution_priority": "立即/择机/观望"
}}"""

        result = self._call_llm(system_prompt, user_prompt)
        
        if result:
            try:
                json_start = result.find('{')
                json_end = result.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    data = json.loads(result[json_start:json_end])
                    
                    signal_map = {
                        "买入": SignalType.BUY,
                        "卖出": SignalType.SELL,
                        "持有": SignalType.HOLD,
                        "观望": SignalType.WATCH
                    }
                    
                    return TradeSuggestion(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        signal=signal_map.get(data.get("signal", "观望"), SignalType.WATCH),
                        confidence=ConfidenceLevel(min(5, max(1, data.get("confidence", 3)))),
                        suggested_amount=data.get("suggested_amount"),
                        reasons=data.get("reasons", []),
                        risk_warnings=data.get("risk_warnings", []),
                        score=fund_score
                    )
            except Exception as e:
                logger.error(f"解析交易建议失败: {e}")
        
        # 返回默认建议
        return self._default_suggestion(fund_code, fund_name, fund_score, position)
    
    def _default_market_analysis(
        self, 
        market_summary: MarketSummary, 
        valuations: List[IndexValuation]
    ) -> Dict[str, Any]:
        """默认市场分析（当AI不可用时）"""
        
        # 计算平均估值百分位
        avg_pe_pct = sum(v.pe_percentile for v in valuations) / len(valuations) if valuations else 50
        
        # 判断估值水平
        if avg_pe_pct < 30:
            valuation_level = "低估"
            max_position = 0.8
        elif avg_pe_pct < 70:
            valuation_level = "正常"
            max_position = 0.6
        else:
            valuation_level = "高估"
            max_position = 0.4
        
        # 判断市场趋势
        avg_change = (market_summary.sh_change + market_summary.hs300_change) / 2
        if avg_change > 0.01:
            market_trend = "上涨"
        elif avg_change < -0.01:
            market_trend = "下跌"
        else:
            market_trend = "震荡"
        
        return {
            "market_trend": market_trend,
            "valuation_level": valuation_level,
            "risk_level": "中",
            "key_observations": [
                f"上证指数{market_summary.sh_change*100:+.2f}%",
                f"平均PE百分位{avg_pe_pct:.1f}%"
            ],
            "operation_suggestion": "根据估值水平适度配置",
            "max_position_ratio": max_position,
            "sectors_to_watch": [],
            "risk_warnings": ["市场有波动风险，请谨慎操作"]
        }
    
    def _default_fund_score(
        self, 
        fund_code: str, 
        fund_name: str, 
        performance: Dict[str, float]
    ) -> FundScore:
        """默认基金评分（当AI不可用时）"""
        score = FundScore(
            fund_code=fund_code,
            fund_name=fund_name
        )
        
        # 基于业绩简单评分
        year_1 = performance.get("year_1", 0) or 0
        year_3 = performance.get("year_3", 0) or 0
        
        # 质量评分（基于长期业绩）
        if year_3 > 0.5:
            score.quality_score = 85
        elif year_3 > 0.3:
            score.quality_score = 75
        elif year_3 > 0.1:
            score.quality_score = 65
        else:
            score.quality_score = 50
        
        # 估值评分（默认中等）
        score.valuation_score = 60
        
        # 趋势评分（基于近1年业绩）
        if year_1 > 0.2:
            score.trend_score = 80
        elif year_1 > 0:
            score.trend_score = 60
        else:
            score.trend_score = 40
        
        # 风险评分（默认中等）
        score.risk_score = 60
        
        score.calculate_total()
        return score
    
    def _default_suggestion(
        self,
        fund_code: str,
        fund_name: str,
        fund_score: FundScore,
        position: Optional[Position]
    ) -> TradeSuggestion:
        """默认交易建议"""
        
        if position:
            # 已持有，判断是否需要卖出
            if position.profit_rate > 0.2:
                return TradeSuggestion(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    signal=SignalType.SELL,
                    confidence=ConfidenceLevel.MEDIUM,
                    reasons=["收益率已达20%，建议止盈"],
                    risk_warnings=["市场可能继续上涨，可分批止盈"],
                    score=fund_score
                )
            elif position.profit_rate < -0.15:
                return TradeSuggestion(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    signal=SignalType.SELL,
                    confidence=ConfidenceLevel.HIGH,
                    reasons=["亏损超过15%，触发止损"],
                    risk_warnings=["止损可能错过反弹"],
                    score=fund_score
                )
            else:
                return TradeSuggestion(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    signal=SignalType.HOLD,
                    confidence=ConfidenceLevel.HIGH,
                    reasons=["持仓收益正常，继续持有"],
                    risk_warnings=[],
                    score=fund_score
                )
        else:
            # 未持有，判断是否买入
            if fund_score.total_score >= 70:
                return TradeSuggestion(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    signal=SignalType.BUY,
                    confidence=ConfidenceLevel.MEDIUM,
                    reasons=[f"综合评分{fund_score.total_score:.1f}分，达到买入标准"],
                    risk_warnings=["投资有风险，建议分批买入"],
                    score=fund_score
                )
            else:
                return TradeSuggestion(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    signal=SignalType.WATCH,
                    confidence=ConfidenceLevel.LOW,
                    reasons=["综合评分不足，暂不建议买入"],
                    risk_warnings=[],
                    score=fund_score
                )


# 创建全局实例
ai_advisor = AIAdvisor()
