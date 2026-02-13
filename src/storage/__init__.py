"""
数据存储模块

包含基金数据、评分结果的持久化存储
"""
from .fund_storage import FundStorage, fund_storage

__all__ = ['FundStorage', 'fund_storage']
