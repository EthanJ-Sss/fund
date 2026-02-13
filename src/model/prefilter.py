"""
预筛选规则模块

实现 4433 法则增强版和其他筛选规则
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from loguru import logger


@dataclass
class PreFilterRule:
    """预筛选规则"""
    name: str
    description: str
    is_required: bool = False  # 是否为必须满足的规则
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        """
        检查规则
        
        Returns:
            (是否通过, 原因说明)
        """
        raise NotImplementedError


class MinScaleRule(PreFilterRule):
    """最小规模规则"""
    def __init__(self, min_scale: float = 2.0):
        super().__init__(
            name='min_scale',
            description=f'基金规模不低于 {min_scale} 亿元',
            is_required=True
        )
        self.min_scale = min_scale
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        scale = fund_data.get('scale', 0)
        if scale and scale >= self.min_scale:
            return True, f'规模 {scale:.2f} 亿元，符合要求'
        return False, f'规模 {scale:.2f} 亿元，低于 {self.min_scale} 亿元'


class MinTenureRule(PreFilterRule):
    """最小成立时间规则"""
    def __init__(self, min_years: float = 3.0):
        super().__init__(
            name='min_tenure',
            description=f'基金成立时间不少于 {min_years} 年',
            is_required=True
        )
        self.min_years = min_years
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        tenure = fund_data.get('tenure_years', 0)
        if tenure and tenure >= self.min_years:
            return True, f'成立 {tenure:.1f} 年，符合要求'
        return False, f'成立 {tenure:.1f} 年，不足 {self.min_years} 年'


class MinManagerTenureRule(PreFilterRule):
    """基金经理最小任职时间规则"""
    def __init__(self, min_years: float = 2.0):
        super().__init__(
            name='min_manager_tenure',
            description=f'基金经理任职时间不少于 {min_years} 年',
            is_required=False
        )
        self.min_years = min_years
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        tenure = fund_data.get('manager_tenure', 0)
        if tenure and tenure >= self.min_years:
            return True, f'基金经理任职 {tenure:.1f} 年，符合要求'
        return False, f'基金经理任职 {tenure:.1f} 年，不足 {self.min_years} 年'


class MaxDrawdownRule(PreFilterRule):
    """最大回撤规则"""
    def __init__(self, max_drawdown: float = -40.0):
        super().__init__(
            name='max_drawdown',
            description=f'最大回撤不超过 {abs(max_drawdown)}%',
            is_required=False
        )
        self.max_drawdown = max_drawdown
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        drawdown = fund_data.get('max_drawdown', 0)
        if drawdown is None or drawdown >= self.max_drawdown:
            return True, f'最大回撤 {drawdown:.1f}%，符合要求'
        return False, f'最大回撤 {drawdown:.1f}%，超过 {abs(self.max_drawdown)}%'


class Rule4433:
    """
    4433 法则
    
    - 第一个4：最近1年收益率排名前1/4
    - 第二个4：最近2年、3年、5年及今年以来收益率排名前1/4
    - 第三个3：最近6个月收益率排名前1/3
    - 第四个3：最近3个月收益率排名前1/3
    """
    
    @staticmethod
    def check_4433(
        rank_pct_1y: float = None,
        rank_pct_2y: float = None,
        rank_pct_3y: float = None,
        rank_pct_6m: float = None,
        rank_pct_3m: float = None,
    ) -> Tuple[bool, List[str]]:
        """
        检查 4433 法则
        
        Args:
            rank_pct_*: 各期排名百分位 (0-100，越小越好)
            
        Returns:
            (是否通过, 不通过的规则列表)
        """
        failures = []
        
        # 第一个4：1年排名前1/4
        if rank_pct_1y is not None and rank_pct_1y > 25:
            failures.append(f'1年排名 {rank_pct_1y:.1f}%，未进前1/4')
        
        # 第二个4：2年、3年排名前1/4
        if rank_pct_2y is not None and rank_pct_2y > 25:
            failures.append(f'2年排名 {rank_pct_2y:.1f}%，未进前1/4')
        if rank_pct_3y is not None and rank_pct_3y > 25:
            failures.append(f'3年排名 {rank_pct_3y:.1f}%，未进前1/4')
        
        # 第三个3：6个月排名前1/3
        if rank_pct_6m is not None and rank_pct_6m > 33.33:
            failures.append(f'6个月排名 {rank_pct_6m:.1f}%，未进前1/3')
        
        # 第四个3：3个月排名前1/3
        if rank_pct_3m is not None and rank_pct_3m > 33.33:
            failures.append(f'3个月排名 {rank_pct_3m:.1f}%，未进前1/3')
        
        return len(failures) == 0, failures


class Enhanced4433Rule(PreFilterRule):
    """
    4433 增强版规则
    
    在原版 4433 基础上增加：
    - 夏普比率 > 1
    - 最大回撤 < 25%
    - 基金经理任职 >= 2年
    """
    
    def __init__(self, strict: bool = False):
        super().__init__(
            name='enhanced_4433',
            description='4433法则增强版',
            is_required=False
        )
        self.strict = strict  # 严格模式要求全部通过
    
    def check(self, fund_data: Dict) -> Tuple[bool, str]:
        passed_rules = []
        failed_rules = []
        
        # 原版 4433 检查
        passed_4433, failures_4433 = Rule4433.check_4433(
            rank_pct_1y=fund_data.get('rank_pct_1y'),
            rank_pct_2y=fund_data.get('rank_pct_2y'),
            rank_pct_3y=fund_data.get('rank_pct_3y'),
            rank_pct_6m=fund_data.get('rank_pct_6m'),
            rank_pct_3m=fund_data.get('rank_pct_3m'),
        )
        
        if passed_4433:
            passed_rules.append('4433法则')
        else:
            failed_rules.extend(failures_4433)
        
        # 夏普比率检查
        sharpe = fund_data.get('sharpe_ratio', 0)
        if sharpe and sharpe > 1:
            passed_rules.append(f'夏普比率 {sharpe:.2f}')
        else:
            failed_rules.append(f'夏普比率 {sharpe:.2f} < 1')
        
        # 最大回撤检查
        max_dd = fund_data.get('max_drawdown', 0)
        if max_dd is None or max_dd > -25:
            passed_rules.append(f'最大回撤 {max_dd:.1f}%')
        else:
            failed_rules.append(f'最大回撤 {max_dd:.1f}% > 25%')
        
        # 基金经理任职检查
        manager_tenure = fund_data.get('manager_tenure', 0)
        if manager_tenure and manager_tenure >= 2:
            passed_rules.append(f'经理任职 {manager_tenure:.1f}年')
        else:
            failed_rules.append(f'经理任职 {manager_tenure:.1f}年 < 2年')
        
        if self.strict:
            passed = len(failed_rules) == 0
        else:
            # 宽松模式：通过至少 3 项即可
            passed = len(passed_rules) >= 3
        
        reason = f"通过: {', '.join(passed_rules)}" if passed_rules else ""
        if failed_rules:
            reason += f" | 未通过: {', '.join(failed_rules)}"
        
        return passed, reason


class PreFilter:
    """
    预筛选器
    
    功能：
    1. 应用多个筛选规则
    2. 必须规则全部通过
    3. 可选规则达到阈值
    4. 输出筛选结果
    """
    
    def __init__(
        self,
        min_scale: float = 2.0,
        min_tenure: float = 3.0,
        min_manager_tenure: float = 2.0,
        max_drawdown: float = -40.0,
        use_4433: bool = True,
        strict_4433: bool = False
    ):
        """
        初始化预筛选器
        
        Args:
            min_scale: 最小规模（亿元）
            min_tenure: 最小成立年限
            min_manager_tenure: 基金经理最小任职年限
            max_drawdown: 最大允许回撤
            use_4433: 是否使用 4433 规则
            strict_4433: 是否使用严格版 4433
        """
        self.rules: List[PreFilterRule] = [
            MinScaleRule(min_scale),
            MinTenureRule(min_tenure),
            MinManagerTenureRule(min_manager_tenure),
            MaxDrawdownRule(max_drawdown),
        ]
        
        if use_4433:
            self.rules.append(Enhanced4433Rule(strict=strict_4433))
    
    def add_rule(self, rule: PreFilterRule):
        """添加自定义规则"""
        self.rules.append(rule)
    
    def filter_single(
        self, 
        fund_data: Dict,
        return_details: bool = False
    ) -> Tuple[bool, Optional[Dict]]:
        """
        筛选单个基金
        
        Args:
            fund_data: 基金数据
            return_details: 是否返回详细结果
            
        Returns:
            (是否通过, 详细结果)
        """
        required_passed = True
        optional_passed = 0
        optional_total = 0
        details = {}
        
        for rule in self.rules:
            passed, reason = rule.check(fund_data)
            details[rule.name] = {
                'passed': passed,
                'reason': reason,
                'required': rule.is_required
            }
            
            if rule.is_required:
                if not passed:
                    required_passed = False
            else:
                optional_total += 1
                if passed:
                    optional_passed += 1
        
        # 必须规则全通过，可选规则至少通过一半
        final_passed = required_passed and (
            optional_total == 0 or optional_passed >= optional_total / 2
        )
        
        if return_details:
            return final_passed, details
        return final_passed, None
    
    def filter_batch(
        self, 
        funds_data: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        批量筛选
        
        Args:
            funds_data: 基金数据列表
            
        Returns:
            (通过的基金列表, 未通过的基金列表)
        """
        passed_funds = []
        failed_funds = []
        
        for fund in funds_data:
            is_passed, details = self.filter_single(fund, return_details=True)
            
            result = {
                **fund,
                'prefilter_passed': is_passed,
                'prefilter_details': details
            }
            
            if is_passed:
                passed_funds.append(result)
            else:
                failed_funds.append(result)
        
        logger.info(f"预筛选完成: 通过 {len(passed_funds)} / 总计 {len(funds_data)}")
        
        return passed_funds, failed_funds
    
    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选 DataFrame
        
        Args:
            df: 包含基金数据的 DataFrame
            
        Returns:
            筛选后的 DataFrame
        """
        if df.empty:
            return df
        
        # 转换为字典列表进行筛选
        funds_data = df.to_dict('records')
        passed_funds, _ = self.filter_batch(funds_data)
        
        if passed_funds:
            result_df = pd.DataFrame(passed_funds)
            # 移除筛选详情列
            if 'prefilter_details' in result_df.columns:
                result_df = result_df.drop(columns=['prefilter_details'])
            return result_df
        
        return pd.DataFrame()
    
    def get_rules_summary(self) -> List[Dict]:
        """获取规则摘要"""
        return [
            {
                'name': rule.name,
                'description': rule.description,
                'required': rule.is_required
            }
            for rule in self.rules
        ]


# 预设配置
class ConservativePreFilter(PreFilter):
    """保守型预筛选器"""
    def __init__(self):
        super().__init__(
            min_scale=5.0,
            min_tenure=5.0,
            min_manager_tenure=3.0,
            max_drawdown=-25.0,
            use_4433=True,
            strict_4433=True
        )


class ModeratePreFilter(PreFilter):
    """稳健型预筛选器"""
    def __init__(self):
        super().__init__(
            min_scale=2.0,
            min_tenure=3.0,
            min_manager_tenure=2.0,
            max_drawdown=-35.0,
            use_4433=True,
            strict_4433=False
        )


class AggressivePreFilter(PreFilter):
    """进取型预筛选器"""
    def __init__(self):
        super().__init__(
            min_scale=1.0,
            min_tenure=2.0,
            min_manager_tenure=1.0,
            max_drawdown=-50.0,
            use_4433=False,
            strict_4433=False
        )


# 创建全局实例
pre_filter = PreFilter()
