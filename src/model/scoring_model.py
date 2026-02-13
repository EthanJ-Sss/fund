"""
综合评分模型

基于多因子加权计算基金综合得分
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


@dataclass
class FactorWeight:
    """因子权重配置"""
    name: str
    weight: float
    higher_is_better: bool = True
    min_value: float = None
    max_value: float = None
    
    def normalize(self, value: float) -> float:
        """归一化到 0-100 分"""
        if pd.isna(value):
            return 50  # 缺失值给中性分
        
        if self.min_value is not None and self.max_value is not None:
            # 线性归一化
            if self.max_value == self.min_value:
                return 50
            normalized = (value - self.min_value) / (self.max_value - self.min_value) * 100
            normalized = max(0, min(100, normalized))
            
            if not self.higher_is_better:
                normalized = 100 - normalized
            
            return normalized
        
        # 如果没有边界，根据方向返回
        if self.higher_is_better:
            return min(100, max(0, value))
        else:
            return min(100, max(0, 100 - value))


@dataclass
class ScoringConfig:
    """评分配置"""
    # 收益因子权重
    return_weight: float = 0.25
    # 风险因子权重
    risk_weight: float = 0.20
    # 风险调整收益权重
    risk_adjusted_weight: float = 0.25
    # 规模因子权重
    scale_weight: float = 0.10
    # 基金经理因子权重
    manager_weight: float = 0.10
    # 风格稳定性权重
    style_weight: float = 0.10
    
    # 子因子配置
    return_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('return_1y', 0.4, True, -50, 100),
        FactorWeight('return_3y', 0.35, True, -50, 200),
        FactorWeight('rank_pct_1y', 0.25, False, 0, 100),  # 百分位越小越好
    ])
    
    risk_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('volatility', 0.4, False, 0, 50),  # 波动率越小越好
        FactorWeight('max_drawdown', 0.4, False, -50, 0),  # 回撤越小越好
        FactorWeight('downside_volatility', 0.2, False, 0, 30),
    ])
    
    risk_adjusted_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('sharpe_ratio', 0.4, True, -1, 3),
        FactorWeight('sortino_ratio', 0.3, True, -1, 4),
        FactorWeight('calmar_ratio', 0.3, True, -1, 3),
    ])
    
    scale_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('scale_score', 1.0, True, 0, 100),
    ])
    
    manager_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('manager_experience_score', 0.5, True, 0, 100),
        FactorWeight('manager_focus_score', 0.5, True, 0, 100),
    ])
    
    style_factors: List[FactorWeight] = field(default_factory=lambda: [
        FactorWeight('style_stability_score', 0.5, True, 0, 100),
        FactorWeight('return_stability', 0.5, True, 0, 100),
    ])


class ScoringModel:
    """
    综合评分模型
    
    功能：
    1. 多因子加权评分
    2. 分类别评分
    3. 综合得分计算
    4. 评分排名
    """
    
    def __init__(self, config: ScoringConfig = None):
        """
        初始化评分模型
        
        Args:
            config: 评分配置
        """
        self.config = config or ScoringConfig()
    
    def calculate_category_score(
        self, 
        factors: Dict[str, float],
        factor_weights: List[FactorWeight]
    ) -> Tuple[float, Dict[str, float]]:
        """
        计算单个类别的得分
        
        Args:
            factors: 因子值字典
            factor_weights: 因子权重列表
            
        Returns:
            (类别得分, 各因子得分明细)
        """
        total_weight = 0
        weighted_sum = 0
        details = {}
        
        for fw in factor_weights:
            if fw.name in factors:
                value = factors[fw.name]
                normalized = fw.normalize(value)
                weighted_sum += normalized * fw.weight
                total_weight += fw.weight
                details[fw.name] = normalized
        
        if total_weight > 0:
            score = weighted_sum / total_weight
        else:
            score = 50  # 默认中性分
        
        return score, details
    
    def calculate_total_score(
        self, 
        factors: Dict[str, float]
    ) -> Dict:
        """
        计算综合得分
        
        Args:
            factors: 所有因子值
            
        Returns:
            评分结果字典
        """
        result = {
            'total_score': 0,
            'category_scores': {},
            'factor_details': {},
        }
        
        # 计算各类别得分
        categories = [
            ('return', self.config.return_weight, self.config.return_factors),
            ('risk', self.config.risk_weight, self.config.risk_factors),
            ('risk_adjusted', self.config.risk_adjusted_weight, self.config.risk_adjusted_factors),
            ('scale', self.config.scale_weight, self.config.scale_factors),
            ('manager', self.config.manager_weight, self.config.manager_factors),
            ('style', self.config.style_weight, self.config.style_factors),
        ]
        
        total_weight = 0
        weighted_sum = 0
        
        for cat_name, cat_weight, factor_weights in categories:
            cat_score, cat_details = self.calculate_category_score(factors, factor_weights)
            result['category_scores'][cat_name] = cat_score
            result['factor_details'][cat_name] = cat_details
            
            weighted_sum += cat_score * cat_weight
            total_weight += cat_weight
        
        if total_weight > 0:
            result['total_score'] = weighted_sum / total_weight
        
        return result
    
    def batch_score(
        self, 
        funds_factors: Dict[str, Dict[str, float]]
    ) -> pd.DataFrame:
        """
        批量计算基金得分
        
        Args:
            funds_factors: {fund_code: factors_dict}
            
        Returns:
            评分结果 DataFrame
        """
        results = []
        
        for fund_code, factors in funds_factors.items():
            score_result = self.calculate_total_score(factors)
            
            row = {
                'fund_code': fund_code,
                'total_score': score_result['total_score'],
            }
            
            # 添加类别得分
            for cat_name, cat_score in score_result['category_scores'].items():
                row[f'{cat_name}_score'] = cat_score
            
            results.append(row)
        
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('total_score', ascending=False).reset_index(drop=True)
            df['rank'] = range(1, len(df) + 1)
        
        return df
    
    def get_score_grade(self, score: float) -> str:
        """
        获取评分等级
        
        Args:
            score: 综合得分
            
        Returns:
            评分等级 (A/B/C/D/E)
        """
        if score >= 80:
            return 'A'
        elif score >= 70:
            return 'B'
        elif score >= 60:
            return 'C'
        elif score >= 50:
            return 'D'
        else:
            return 'E'
    
    def get_recommendation(self, score: float, factors: Dict[str, float]) -> Dict:
        """
        获取投资建议
        
        Args:
            score: 综合得分
            factors: 因子值
            
        Returns:
            投资建议
        """
        grade = self.get_score_grade(score)
        
        recommendation = {
            'grade': grade,
            'score': score,
            'action': '',
            'reasons': [],
            'risks': [],
        }
        
        # 根据等级给出建议
        if grade == 'A':
            recommendation['action'] = '强烈推荐'
            recommendation['reasons'].append('综合评分优秀，各项指标表现出色')
        elif grade == 'B':
            recommendation['action'] = '推荐'
            recommendation['reasons'].append('综合评分良好，适合配置')
        elif grade == 'C':
            recommendation['action'] = '可考虑'
            recommendation['reasons'].append('综合评分中等，可作为补充配置')
        elif grade == 'D':
            recommendation['action'] = '谨慎'
            recommendation['reasons'].append('综合评分偏低，需谨慎考虑')
        else:
            recommendation['action'] = '不推荐'
            recommendation['reasons'].append('综合评分较差，不建议投资')
        
        # 分析具体因子，添加详细建议
        # 收益分析
        return_1y = factors.get('return_1y', 0)
        if return_1y > 30:
            recommendation['reasons'].append(f'近一年收益优秀 ({return_1y:.1f}%)')
        elif return_1y < 0:
            recommendation['risks'].append(f'近一年收益为负 ({return_1y:.1f}%)')
        
        # 风险分析
        max_dd = factors.get('max_drawdown', 0)
        if max_dd < -30:
            recommendation['risks'].append(f'最大回撤较大 ({max_dd:.1f}%)')
        
        volatility = factors.get('volatility', 0)
        if volatility > 30:
            recommendation['risks'].append(f'波动率较高 ({volatility:.1f}%)')
        
        # 夏普比率分析
        sharpe = factors.get('sharpe_ratio', 0)
        if sharpe > 1.5:
            recommendation['reasons'].append(f'风险调整收益出色 (夏普比率: {sharpe:.2f})')
        elif sharpe < 0.5:
            recommendation['risks'].append(f'风险调整收益偏低 (夏普比率: {sharpe:.2f})')
        
        # 基金经理分析
        manager_tenure = factors.get('manager_tenure', 0)
        if manager_tenure >= 5:
            recommendation['reasons'].append(f'基金经理经验丰富 ({manager_tenure:.1f}年)')
        elif manager_tenure < 2:
            recommendation['risks'].append(f'基金经理任期较短 ({manager_tenure:.1f}年)')
        
        # 规模分析
        scale = factors.get('scale', 0)
        if scale < 2:
            recommendation['risks'].append(f'基金规模偏小 ({scale:.1f}亿)')
        elif scale > 200:
            recommendation['risks'].append(f'基金规模过大可能影响灵活性 ({scale:.1f}亿)')
        
        return recommendation


# 针对不同类型基金的评分配置
class IndexFundScoringConfig(ScoringConfig):
    """指数基金评分配置"""
    def __init__(self):
        super().__init__()
        # 指数基金更注重跟踪误差和费率
        self.return_weight = 0.30
        self.risk_weight = 0.15
        self.risk_adjusted_weight = 0.20
        self.scale_weight = 0.15
        self.manager_weight = 0.05
        self.style_weight = 0.15


class ActiveFundScoringConfig(ScoringConfig):
    """主动型基金评分配置"""
    def __init__(self):
        super().__init__()
        # 主动基金更注重基金经理和超额收益
        self.return_weight = 0.20
        self.risk_weight = 0.15
        self.risk_adjusted_weight = 0.30
        self.scale_weight = 0.10
        self.manager_weight = 0.15
        self.style_weight = 0.10


class BondFundScoringConfig(ScoringConfig):
    """债券基金评分配置"""
    def __init__(self):
        super().__init__()
        # 债券基金更注重风险控制和稳定性
        self.return_weight = 0.20
        self.risk_weight = 0.30
        self.risk_adjusted_weight = 0.25
        self.scale_weight = 0.10
        self.manager_weight = 0.05
        self.style_weight = 0.10


def get_scoring_config_for_type(fund_type: str) -> ScoringConfig:
    """
    根据基金类型获取对应的评分配置
    
    Args:
        fund_type: 基金类型
        
    Returns:
        对应的评分配置
    """
    type_lower = fund_type.lower() if fund_type else ''
    
    if '指数' in type_lower or 'index' in type_lower:
        return IndexFundScoringConfig()
    elif '债券' in type_lower or 'bond' in type_lower:
        return BondFundScoringConfig()
    elif '股票' in type_lower or '混合' in type_lower:
        return ActiveFundScoringConfig()
    else:
        return ScoringConfig()


# 创建全局实例
scoring_model = ScoringModel()
