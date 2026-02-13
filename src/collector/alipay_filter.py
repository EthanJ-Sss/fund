"""
支付宝可购基金过滤器

过滤出支付宝（蚂蚁财富）平台上可购买的基金
"""
from typing import Dict, List, Set

import pandas as pd
from loguru import logger


class AlipayFundFilter:
    """
    支付宝可购基金过滤器
    
    支付宝（蚂蚁财富）平台上可购买的基金需满足以下条件：
    1. 公募基金（非私募）
    2. 开放申购状态
    3. 非QDII（部分QDII可能受限）
    4. 非分级基金（已整改）
    5. 规模通常 > 5000万
    
    由于蚂蚁财富没有公开API，我们通过以下方式确定可购基金：
    1. 从天天基金获取开放申购的基金列表
    2. 排除特定类型（封闭期基金、定开基金等）
    3. 定期更新可购基金白名单
    """
    
    # 需要排除的基金类型关键词
    EXCLUDE_KEYWORDS = [
        '封闭', '定开', '定期开放', '持有期',
        '三年', '五年', '两年', '一年',
        'LOF', '分级', '理财',
    ]
    
    # 需要排除的基金代码前缀（某些特殊类型）
    EXCLUDE_PREFIXES = [
        '5',   # 场内基金（ETF）- 支付宝可购买联接基金但不能买场内ETF
    ]
    
    # 允许的基金类型
    ALLOWED_TYPES = [
        '股票型', '混合型', '债券型', '指数型',
        '货币型', 'QDII', 'FOF',
        '股票指数', '联接基金', '联接',
        '股票型-', '混合型-', '债券型-',
    ]
    
    def __init__(self):
        self._purchasable_funds: Set[str] = set()
        self._last_update = None
    
    def filter_purchasable(self, fund_list: pd.DataFrame) -> pd.DataFrame:
        """
        过滤出支付宝可购买的基金
        
        Args:
            fund_list: 完整基金列表，需包含 'code'/'基金代码', 'name'/'基金简称', 'type'/'基金类型' 列
            
        Returns:
            过滤后的可购基金列表
        """
        if fund_list.empty:
            return fund_list
        
        # 标准化列名
        df = fund_list.copy()
        column_mapping = {
            '基金代码': 'code',
            '基金简称': 'name',
            '基金类型': 'type'
        }
        df = df.rename(columns=column_mapping)
        
        # 确保必要的列存在
        if 'code' not in df.columns or 'name' not in df.columns:
            logger.warning("基金列表缺少必要的列")
            return fund_list
        
        mask = pd.Series([True] * len(df), index=df.index)
        
        # 1. 排除特定关键词
        for keyword in self.EXCLUDE_KEYWORDS:
            mask &= ~df['name'].str.contains(keyword, na=False)
        
        # 2. 排除特定前缀
        for prefix in self.EXCLUDE_PREFIXES:
            mask &= ~df['code'].astype(str).str.startswith(prefix)
        
        # 3. 只保留主要基金类型（如果有 type 列）
        if 'type' in df.columns:
            def check_type(t):
                if pd.isna(t):
                    return True  # 类型未知的暂时保留
                return any(at in str(t) for at in self.ALLOWED_TYPES)
            
            mask &= df['type'].apply(check_type)
        
        result = fund_list[mask].copy()
        logger.info(f"过滤后可购基金数量: {len(result)} / {len(fund_list)}")
        
        return result
    
    def categorize_funds(self, fund_list: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        将基金按类型分类（用于分类筛选）
        
        Args:
            fund_list: 基金列表
            
        Returns:
            Dict[category, DataFrame]
        """
        # 标准化列名
        df = fund_list.copy()
        column_mapping = {
            '基金代码': 'code',
            '基金简称': 'name',
            '基金类型': 'type'
        }
        df = df.rename(columns=column_mapping)
        
        if 'type' not in df.columns:
            return {'all': df}
        
        categories = {
            'stock': df[df['type'].str.contains('股票', na=False)],
            'mixed': df[df['type'].str.contains('混合', na=False)],
            'bond': df[df['type'].str.contains('债券', na=False)],
            'index': df[df['type'].str.contains('指数|联接', na=False)],
            'money': df[df['type'].str.contains('货币', na=False)],
            'qdii': df[df['type'].str.contains('QDII', na=False)],
            'fof': df[df['type'].str.contains('FOF', na=False)],
        }
        
        return categories
    
    def get_fund_type(self, fund_name: str, fund_type: str = None) -> str:
        """
        判断基金类型
        
        Args:
            fund_name: 基金名称
            fund_type: 原始类型字符串
            
        Returns:
            标准化类型: stock/mixed/bond/index/money/qdii/fof/other
        """
        name = fund_name.lower() if fund_name else ''
        type_str = str(fund_type).lower() if fund_type else ''
        
        combined = name + ' ' + type_str
        
        if '货币' in combined or '现金' in combined:
            return 'money'
        elif '指数' in combined or 'etf' in combined or '联接' in combined:
            return 'index'
        elif 'qdii' in combined or '海外' in combined:
            return 'qdii'
        elif 'fof' in combined:
            return 'fof'
        elif '债券' in combined or '债' in combined:
            return 'bond'
        elif '股票' in combined:
            return 'stock'
        elif '混合' in combined or '灵活' in combined or '配置' in combined:
            return 'mixed'
        else:
            return 'other'


# 创建全局实例
alipay_filter = AlipayFundFilter()
