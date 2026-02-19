"""
投资组合顾问模块

基于核心-卫星策略和市场估值生成投资组合建议
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from loguru import logger

from ..collector.index_valuation import IndexValuationCollector
from ..model.scoring_model import ScoringModel

class PortfolioAdvisor:
    """
    投资组合顾问
    
    功能：
    1. 市场估值分析
    2. 资产配置建议
    3. 基金组合构建
    """
    
    def __init__(self, valuation_collector: IndexValuationCollector = None):
        self.valuation = valuation_collector or IndexValuationCollector()
        
    def suggest_portfolio(
        self, 
        top_funds: Dict[str, List[Dict]], 
        risk_level: str = 'balanced'
    ) -> Dict:
        """
        生成投资组合建议
        
        Args:
            top_funds: 各类型排名靠前的基金 {type: [funds]}
            risk_level: 风险偏好 (conservative/balanced/aggressive)
            
        Returns:
            组合建议字典
        """
        # 1. 获取市场估值状态
        market_status = self._analyze_market_status()
        
        # 2. 确定大类资产配置比例
        allocation = self._get_asset_allocation(market_status, risk_level)
        
        # 3. 筛选具体基金
        portfolio_funds = self._select_funds(top_funds, allocation)
        
        # 4. 构建返回结果
        result = {
            'market_status': market_status,
            'allocation_plan': allocation,
            'portfolio': portfolio_funds,
            'strategy_description': self._generate_strategy_description(market_status, allocation)
        }
        
        return result
    
    def _analyze_market_status(self) -> Dict:
        """分析市场估值状态"""
        try:
            # 获取沪深300估值
            hs300 = self.valuation.get_hs300_valuation()
            
            if hs300:
                pe_pct = hs300.pe_percentile
                pe_val = hs300.pe
            else:
                # 默认中性
                pe_pct = 50
                pe_val = 12.5
                
            if pe_pct < 20:
                status = "低估"
                signal = "积极买入"
            elif pe_pct < 40:
                status = "偏低"
                signal = "分批买入"
            elif pe_pct < 60:
                status = "正常"
                signal = "定投持有"
            elif pe_pct < 80:
                status = "偏高"
                signal = "谨慎持有"
            else:
                status = "高估"
                signal = "分批止盈"
                
            return {
                'pe_percentile': pe_pct,
                'pe_value': pe_val,
                'status': status,
                'signal': signal
            }
        except Exception as e:
            logger.error(f"市场分析失败: {e}")
            return {
                'pe_percentile': 50,
                'pe_value': 0,
                'status': "未知",
                'signal': "稳健操作"
            }
            
    def _get_asset_allocation(self, market_status: Dict, risk_level: str) -> Dict:
        """根据市场状态和风险偏好确定配置"""
        pe_pct = market_status.get('pe_percentile', 50)
        
        # 基础权益仓位（根据风险偏好）
        base_equity = {
            'conservative': 0.2,
            'balanced': 0.5,
            'aggressive': 0.8
        }.get(risk_level, 0.5)
        
        # 根据市场估值调整权益仓位
        # 估值越低，仓位越高
        valuation_adj = (50 - pe_pct) / 100 * 0.4  # 最大调整 +/- 20%
        
        target_equity = max(0.1, min(0.95, base_equity + valuation_adj))
        target_bond = 1.0 - target_equity
        
        # 细分权益仓位：核心 vs 卫星
        # 核心（宽基指数/稳健混合）：60-70%
        # 卫星（行业主题/积极股票）：30-40%
        core_satellite_ratio = 0.7
        
        return {
            'equity_ratio': round(target_equity, 2),
            'bond_ratio': round(target_bond, 2),
            'core_ratio': round(target_equity * core_satellite_ratio, 2),
            'satellite_ratio': round(target_equity * (1 - core_satellite_ratio), 2)
        }
        
    def _select_funds(self, top_funds: Dict[str, List[Dict]], allocation: Dict) -> List[Dict]:
        """选择具体基金构建组合"""
        portfolio = []
        
        # 1. 选择核心资产（指数型）
        # 优先选择沪深300、中证500等宽基
        index_funds = top_funds.get('指数型', [])
        if index_funds:
            # 简单策略：选得分最高的1只
            core_fund = index_funds[0]
            portfolio.append({
                'role': '核心',
                'fund_code': core_fund.get('fund_code'),
                'fund_name': core_fund.get('fund_name'),
                'weight': allocation['core_ratio'],
                'reason': '宽基指数，作为组合压舱石'
            })
            
        # 2. 选择卫星资产（股票型/混合型）
        # 选择得分最高的2只，最好风格不同（这里简化为直接选前2名）
        active_funds = top_funds.get('股票型', []) + top_funds.get('混合型', [])
        # 按分数排序
        active_funds.sort(key=lambda x: x.get('total_score', 0), reverse=True)
        
        if active_funds:
            sat_count = 2
            weight_per_sat = allocation['satellite_ratio'] / sat_count
            
            for i in range(min(sat_count, len(active_funds))):
                fund = active_funds[i]
                portfolio.append({
                    'role': '卫星',
                    'fund_code': fund.get('fund_code'),
                    'fund_name': fund.get('fund_name'),
                    'weight': round(weight_per_sat, 2),
                    'reason': '优质主动基金，追求超额收益'
                })
                
        # 3. 选择债券资产（债券型）
        bond_funds = top_funds.get('债券型', [])
        if bond_funds and allocation['bond_ratio'] > 0.05:
            bond_fund = bond_funds[0]
            portfolio.append({
                'role': '防守',
                'fund_code': bond_fund.get('fund_code'),
                'fund_name': bond_fund.get('fund_name'),
                'weight': allocation['bond_ratio'],
                'reason': '债券基金，降低组合波动'
            })
            
        return portfolio
    
    def _generate_strategy_description(self, market_status: Dict, allocation: Dict) -> str:
        """生成策略说明"""
        status = market_status['status']
        equity_pct = allocation['equity_ratio'] * 100
        
        desc = f"当前市场估值{status}（PE百分位 {market_status['pe_percentile']:.1f}%）。"
        desc += f"建议采用{'进攻' if equity_pct > 60 else '防御' if equity_pct < 40 else '平衡'}策略，"
        desc += f"权益类资产配置 {equity_pct:.0f}%，固收类配置 {allocation['bond_ratio']*100:.0f}%。"
        desc += "核心部分配置宽基指数以把握市场平均收益，卫星部分配置优质主动基金以追求超额收益。"
        
        return desc
    
# 全局实例
portfolio_advisor = PortfolioAdvisor()
