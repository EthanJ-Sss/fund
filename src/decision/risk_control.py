"""
风险控制模块
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from loguru import logger

from config.settings import settings
from ..models import Portfolio, TradeSuggestion, SignalType, ConfidenceLevel, MarketSummary


class RiskController:
    """风险控制器"""
    
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.config = settings.RiskControl
        
        # 熔断状态
        self._circuit_breaker_active = False
        self._circuit_breaker_reason = ""
        self._circuit_breaker_until = None
    
    def check_all_risks(
        self,
        suggestion: TradeSuggestion,
        market_summary: Optional[MarketSummary] = None
    ) -> Tuple[bool, List[str], List[str]]:
        """综合风险检查
        
        Returns:
            (is_allowed, warnings, blockers)
            - is_allowed: 是否允许执行
            - warnings: 警告信息列表
            - blockers: 阻止执行的原因列表
        """
        warnings = []
        blockers = []
        
        # 1. 检查熔断状态
        if self._circuit_breaker_active:
            if self._circuit_breaker_until and datetime.now() < self._circuit_breaker_until:
                blockers.append(f"熔断中: {self._circuit_breaker_reason}")
                return False, warnings, blockers
            else:
                self._reset_circuit_breaker()
        
        # 2. 检查市场异常
        if market_summary:
            market_check = self._check_market_risk(market_summary)
            if market_check["has_risk"]:
                if market_check["severity"] == "high":
                    blockers.append(market_check["message"])
                else:
                    warnings.append(market_check["message"])
        
        # 3. 根据交易类型检查
        if suggestion.signal == SignalType.BUY:
            buy_check = self._check_buy_risks(suggestion)
            warnings.extend(buy_check["warnings"])
            blockers.extend(buy_check["blockers"])
        elif suggestion.signal == SignalType.SELL:
            sell_check = self._check_sell_risks(suggestion)
            warnings.extend(sell_check["warnings"])
            blockers.extend(sell_check["blockers"])
        
        # 4. 检查置信度
        if suggestion.confidence.value < 3:
            warnings.append(f"建议置信度较低({suggestion.get_confidence_stars()})，建议观望")
        
        is_allowed = len(blockers) == 0
        return is_allowed, warnings, blockers
    
    def _check_market_risk(self, market_summary: MarketSummary) -> Dict:
        """检查市场风险"""
        result = {"has_risk": False, "severity": "low", "message": ""}
        
        # 检查单日大跌
        if market_summary.hs300_change < self.config.CIRCUIT_BREAKER["market_crash"]:
            result = {
                "has_risk": True,
                "severity": "high",
                "message": f"沪深300单日跌幅{market_summary.hs300_change*100:.2f}%，触发市场熔断"
            }
            self._trigger_circuit_breaker(result["message"], hours=24)
        elif market_summary.hs300_change < -0.02:
            result = {
                "has_risk": True,
                "severity": "medium",
                "message": f"市场大幅下跌({market_summary.hs300_change*100:.2f}%)，建议谨慎操作"
            }
        
        return result
    
    def _check_buy_risks(self, suggestion: TradeSuggestion) -> Dict[str, List[str]]:
        """检查买入风险"""
        warnings = []
        blockers = []
        
        amount = suggestion.suggested_amount or 0
        fund_code = suggestion.fund_code
        
        # 检查现金是否充足
        if amount > self.portfolio.cash:
            blockers.append(f"现金不足: 需要¥{amount:,.2f}，可用¥{self.portfolio.cash:,.2f}")
        
        # 检查单只基金仓位限制
        current_ratio = self.portfolio.get_position_ratio(fund_code)
        new_ratio = current_ratio + (amount / self.portfolio.total_value if self.portfolio.total_value > 0 else 0)
        if new_ratio > self.config.MAX_SINGLE_FUND_POSITION:
            blockers.append(
                f"单只基金仓位超限: 当前{current_ratio*100:.1f}% + 新增{(amount/self.portfolio.total_value)*100:.1f}% = {new_ratio*100:.1f}% > {self.config.MAX_SINGLE_FUND_POSITION*100}%"
            )
        
        # 检查单次买入比例
        if self.portfolio.cash > 0 and amount > self.portfolio.cash * self.config.MAX_SINGLE_BUY_RATIO:
            warnings.append(
                f"单次买入金额较大: ¥{amount:,.2f} 占可用资金的{amount/self.portfolio.cash*100:.1f}%"
            )
        
        # 检查总仓位
        total_position = self.portfolio.get_total_position_ratio()
        if total_position > 0.8:
            warnings.append(f"当前总仓位{total_position*100:.1f}%，已接近上限")
        
        return {"warnings": warnings, "blockers": blockers}
    
    def _check_sell_risks(self, suggestion: TradeSuggestion) -> Dict[str, List[str]]:
        """检查卖出风险"""
        warnings = []
        blockers = []
        
        fund_code = suggestion.fund_code
        position = self.portfolio.get_position(fund_code)
        
        if not position:
            blockers.append(f"未持有基金 {fund_code}")
            return {"warnings": warnings, "blockers": blockers}
        
        # 检查持有天数（短期卖出可能有较高赎回费）
        if position.hold_days < 7:
            warnings.append(f"持有仅{position.hold_days}天，赎回费可能较高")
        
        # 检查是否止损
        if position.profit_rate < self.config.STOP_LOSS_THRESHOLD:
            warnings.append(f"当前亏损{position.profit_rate*100:.2f}%，已触及止损线")
        
        return {"warnings": warnings, "blockers": blockers}
    
    def check_portfolio_health(self) -> Dict:
        """检查投资组合健康状态"""
        health_report = {
            "overall_status": "健康",
            "issues": [],
            "recommendations": []
        }
        
        # 1. 检查总收益
        total_profit_rate = 0
        total_cost = sum(p.shares * p.cost_price for p in self.portfolio.positions)
        if total_cost > 0:
            total_profit = sum(p.profit_loss for p in self.portfolio.positions)
            total_profit_rate = total_profit / total_cost
        
        if total_profit_rate < self.config.CIRCUIT_BREAKER["monthly_loss"]:
            health_report["overall_status"] = "需关注"
            health_report["issues"].append(f"总收益率{total_profit_rate*100:.2f}%，接近月度熔断线")
            health_report["recommendations"].append("建议减仓降低风险")
        
        # 2. 检查集中度
        for pos in self.portfolio.positions:
            ratio = self.portfolio.get_position_ratio(pos.fund_code)
            if ratio > self.config.MAX_SINGLE_FUND_POSITION * 0.9:
                health_report["issues"].append(
                    f"{pos.fund_name}仓位{ratio*100:.1f}%，接近上限"
                )
                health_report["recommendations"].append(f"考虑减持{pos.fund_name}")
        
        # 3. 检查亏损持仓
        losing_positions = [
            p for p in self.portfolio.positions 
            if p.profit_rate < -0.10
        ]
        if losing_positions:
            health_report["issues"].append(
                f"有{len(losing_positions)}只基金亏损超过10%"
            )
            for p in losing_positions:
                health_report["recommendations"].append(
                    f"检视{p.fund_name}，考虑是否止损"
                )
        
        # 4. 更新总体状态
        if len(health_report["issues"]) > 2:
            health_report["overall_status"] = "需关注"
        elif len(health_report["issues"]) > 0:
            health_report["overall_status"] = "一般"
        
        return health_report
    
    def get_max_buy_amount(
        self,
        fund_code: str,
        market_pe_percentile: float = 50
    ) -> float:
        """计算最大可买入金额"""
        
        # 根据估值确定最大仓位
        max_total_position = self._get_max_position_by_valuation(market_pe_percentile)
        
        # 当前仓位
        current_position = self.portfolio.get_total_position_ratio()
        
        # 可增加的仓位
        available_position_ratio = max(0, max_total_position - current_position)
        
        # 该基金已有仓位
        fund_position = self.portfolio.get_position_ratio(fund_code)
        
        # 该基金可增加的仓位
        fund_available = self.config.MAX_SINGLE_FUND_POSITION - fund_position
        
        # 取两者最小值
        final_ratio = min(available_position_ratio, fund_available)
        
        # 计算金额
        max_by_position = self.portfolio.total_value * final_ratio
        
        # 还要考虑单次买入限制
        max_by_single_trade = self.portfolio.cash * self.config.MAX_SINGLE_BUY_RATIO
        
        # 还要考虑可用现金
        max_by_cash = self.portfolio.cash
        
        return min(max_by_position, max_by_single_trade, max_by_cash)
    
    def _get_max_position_by_valuation(self, pe_percentile: float) -> float:
        """根据估值百分位获取最大仓位"""
        for threshold, max_pos in sorted(self.config.VALUATION_POSITION_MAP.items()):
            if pe_percentile < threshold:
                return max_pos
        return 0.20  # 默认最保守
    
    def _trigger_circuit_breaker(self, reason: str, hours: int = 24):
        """触发熔断"""
        self._circuit_breaker_active = True
        self._circuit_breaker_reason = reason
        self._circuit_breaker_until = datetime.now() + timedelta(hours=hours)
        logger.warning(f"熔断触发: {reason}，将持续{hours}小时")
    
    def _reset_circuit_breaker(self):
        """重置熔断"""
        self._circuit_breaker_active = False
        self._circuit_breaker_reason = ""
        self._circuit_breaker_until = None
        logger.info("熔断已解除")
    
    def get_risk_summary(self) -> Dict:
        """获取风险摘要"""
        return {
            "circuit_breaker_active": self._circuit_breaker_active,
            "circuit_breaker_reason": self._circuit_breaker_reason,
            "circuit_breaker_until": self._circuit_breaker_until.isoformat() if self._circuit_breaker_until else None,
            "total_position_ratio": self.portfolio.get_total_position_ratio(),
            "portfolio_health": self.check_portfolio_health()
        }
