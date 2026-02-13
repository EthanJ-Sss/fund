"""
基金数据处理器

计算各类投资指标和风险指标
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


class FundDataProcessor:
    """
    基金数据处理器
    
    功能：
    1. 净值数据清洗和标准化
    2. 收益率计算（日、周、月、年）
    3. 风险指标计算（波动率、最大回撤、夏普比率等）
    4. Alpha/Beta 计算
    5. 排名百分位计算
    """
    
    # 年化天数
    TRADING_DAYS_PER_YEAR = 250
    
    # 无风险利率（年化）
    RISK_FREE_RATE = 0.025  # 2.5%
    
    def __init__(self, risk_free_rate: float = 0.025):
        """
        初始化处理器
        
        Args:
            risk_free_rate: 无风险利率（年化）
        """
        self.risk_free_rate = risk_free_rate
    
    def process_nav_data(self, nav_data: pd.DataFrame) -> pd.DataFrame:
        """
        处理净值数据，标准化格式
        
        Args:
            nav_data: 原始净值数据，支持多种格式
            
        Returns:
            标准化的 DataFrame with columns: date, nav, acc_nav
        """
        if nav_data.empty:
            return pd.DataFrame(columns=['date', 'nav', 'acc_nav'])
        
        df = nav_data.copy()
        
        # 尝试识别日期列
        date_cols = ['净值日期', 'date', '日期', 'FSRQ', 'x']
        date_col = None
        for col in date_cols:
            if col in df.columns:
                date_col = col
                break
        
        # 尝试识别净值列
        nav_cols = ['单位净值', 'nav', 'DWJZ', 'y']
        nav_col = None
        for col in nav_cols:
            if col in df.columns:
                nav_col = col
                break
        
        # 尝试识别累计净值列
        acc_nav_cols = ['累计净值', 'acc_nav', 'LJJZ']
        acc_nav_col = None
        for col in acc_nav_cols:
            if col in df.columns:
                acc_nav_col = col
                break
        
        if date_col is None or nav_col is None:
            logger.warning("无法识别净值数据格式")
            return pd.DataFrame(columns=['date', 'nav', 'acc_nav'])
        
        # 构建标准化 DataFrame
        result = pd.DataFrame()
        result['date'] = pd.to_datetime(df[date_col])
        result['nav'] = pd.to_numeric(df[nav_col], errors='coerce')
        
        if acc_nav_col:
            result['acc_nav'] = pd.to_numeric(df[acc_nav_col], errors='coerce')
        else:
            result['acc_nav'] = result['nav']
        
        # 去重、排序、去空值
        result = result.dropna(subset=['date', 'nav'])
        result = result.drop_duplicates(subset=['date'])
        result = result.sort_values('date').reset_index(drop=True)
        
        return result
    
    def calculate_returns(self, nav_data: pd.DataFrame, periods: List[str] = None) -> Dict[str, float]:
        """
        计算各期收益率
        
        Args:
            nav_data: 标准化的净值数据
            periods: 计算周期列表 ['1d', '1w', '1m', '3m', '6m', '1y', '2y', '3y', 'ytd']
            
        Returns:
            Dict[period, return_pct]
        """
        if nav_data.empty:
            return {}
        
        if periods is None:
            periods = ['1d', '1w', '1m', '3m', '6m', '1y', '2y', '3y', 'ytd']
        
        df = nav_data.copy()
        if 'date' not in df.columns:
            return {}
        
        df = df.sort_values('date')
        latest_nav = df['nav'].iloc[-1]
        latest_date = df['date'].iloc[-1]
        
        returns = {}
        
        period_days = {
            '1d': 1,
            '1w': 7,
            '1m': 30,
            '3m': 90,
            '6m': 180,
            '1y': 365,
            '2y': 730,
            '3y': 1095,
            '5y': 1825,
        }
        
        for period in periods:
            if period == 'ytd':
                # 今年以来
                year_start = datetime(latest_date.year, 1, 1)
                start_data = df[df['date'] <= year_start].tail(1)
            elif period in period_days:
                days = period_days[period]
                target_date = latest_date - timedelta(days=days)
                start_data = df[df['date'] <= target_date].tail(1)
            else:
                continue
            
            if not start_data.empty:
                start_nav = start_data['nav'].iloc[0]
                if start_nav > 0:
                    returns[period] = (latest_nav / start_nav - 1) * 100
        
        return returns
    
    def calculate_risk_metrics(self, nav_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算风险指标
        
        Args:
            nav_data: 标准化的净值数据
            
        Returns:
            Dict containing:
            - volatility: 年化波动率
            - max_drawdown: 最大回撤
            - sharpe_ratio: 夏普比率
            - sortino_ratio: 索提诺比率
            - calmar_ratio: 卡尔马比率
        """
        if nav_data.empty or len(nav_data) < 30:
            return {}
        
        df = nav_data.copy().sort_values('date')
        
        # 计算日收益率
        df['daily_return'] = df['nav'].pct_change()
        
        daily_returns = df['daily_return'].dropna()
        
        if len(daily_returns) < 20:
            return {}
        
        metrics = {}
        
        # 年化波动率
        daily_volatility = daily_returns.std()
        annual_volatility = daily_volatility * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        metrics['volatility'] = annual_volatility * 100
        
        # 最大回撤
        cumulative = (1 + daily_returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        metrics['max_drawdown'] = max_drawdown * 100
        
        # 年化收益率（用于夏普比率）
        total_return = df['nav'].iloc[-1] / df['nav'].iloc[0] - 1
        years = len(daily_returns) / self.TRADING_DAYS_PER_YEAR
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        # 夏普比率
        if annual_volatility > 0:
            sharpe = (annual_return - self.risk_free_rate) / annual_volatility
            metrics['sharpe_ratio'] = sharpe
        else:
            metrics['sharpe_ratio'] = 0
        
        # 索提诺比率（只考虑下行波动）
        negative_returns = daily_returns[daily_returns < 0]
        if len(negative_returns) > 0:
            downside_volatility = negative_returns.std() * np.sqrt(self.TRADING_DAYS_PER_YEAR)
            if downside_volatility > 0:
                sortino = (annual_return - self.risk_free_rate) / downside_volatility
                metrics['sortino_ratio'] = sortino
            else:
                metrics['sortino_ratio'] = 0
        else:
            metrics['sortino_ratio'] = float('inf')
        
        # 卡尔马比率
        if max_drawdown != 0:
            calmar = annual_return / abs(max_drawdown)
            metrics['calmar_ratio'] = calmar
        else:
            metrics['calmar_ratio'] = float('inf')
        
        return metrics
    
    def calculate_alpha_beta(
        self, 
        nav_data: pd.DataFrame, 
        benchmark_data: pd.DataFrame
    ) -> Tuple[float, float]:
        """
        计算 Alpha 和 Beta
        
        Args:
            nav_data: 基金净值数据
            benchmark_data: 基准指数数据（需包含 date, close 或 nav 列）
            
        Returns:
            (alpha, beta)
        """
        if nav_data.empty or benchmark_data.empty:
            return 0.0, 1.0
        
        fund_df = nav_data.copy().sort_values('date')
        bench_df = benchmark_data.copy().sort_values('date')
        
        # 标准化基准数据
        if 'close' in bench_df.columns:
            bench_df['nav'] = bench_df['close']
        
        # 合并数据
        fund_df = fund_df.set_index('date')
        bench_df = bench_df.set_index('date')
        
        merged = fund_df[['nav']].join(bench_df[['nav']], lsuffix='_fund', rsuffix='_bench')
        merged = merged.dropna()
        
        if len(merged) < 30:
            return 0.0, 1.0
        
        # 计算收益率
        fund_returns = merged['nav_fund'].pct_change().dropna()
        bench_returns = merged['nav_bench'].pct_change().dropna()
        
        # 对齐数据
        min_len = min(len(fund_returns), len(bench_returns))
        fund_returns = fund_returns.tail(min_len)
        bench_returns = bench_returns.tail(min_len)
        
        if len(fund_returns) < 20:
            return 0.0, 1.0
        
        # 计算 Beta
        covariance = np.cov(fund_returns, bench_returns)[0][1]
        benchmark_variance = np.var(bench_returns)
        
        if benchmark_variance > 0:
            beta = covariance / benchmark_variance
        else:
            beta = 1.0
        
        # 计算 Alpha（年化）
        fund_annual_return = (1 + fund_returns.mean()) ** self.TRADING_DAYS_PER_YEAR - 1
        bench_annual_return = (1 + bench_returns.mean()) ** self.TRADING_DAYS_PER_YEAR - 1
        
        alpha = fund_annual_return - (self.risk_free_rate + beta * (bench_annual_return - self.risk_free_rate))
        
        return alpha * 100, beta
    
    def calculate_ranking_percentile(
        self, 
        fund_return: float, 
        all_returns: List[float],
        higher_is_better: bool = True
    ) -> float:
        """
        计算排名百分位
        
        Args:
            fund_return: 该基金的收益率
            all_returns: 所有同类基金的收益率列表
            higher_is_better: 是否越高越好
            
        Returns:
            排名百分位 (0-100)，值越小表示排名越靠前
        """
        if not all_returns or pd.isna(fund_return):
            return 50.0
        
        all_returns = [r for r in all_returns if not pd.isna(r)]
        if not all_returns:
            return 50.0
        
        sorted_returns = sorted(all_returns, reverse=higher_is_better)
        
        # 找到当前基金的排名
        rank = 1
        for r in sorted_returns:
            if higher_is_better:
                if fund_return >= r:
                    break
            else:
                if fund_return <= r:
                    break
            rank += 1
        
        # 计算百分位
        percentile = (rank / len(sorted_returns)) * 100
        
        return percentile
    
    def calculate_all_metrics(
        self, 
        nav_data: pd.DataFrame,
        benchmark_data: pd.DataFrame = None
    ) -> Dict:
        """
        计算所有指标的汇总方法
        
        Args:
            nav_data: 净值数据
            benchmark_data: 基准数据（可选）
            
        Returns:
            包含所有指标的字典
        """
        # 处理净值数据
        processed_nav = self.process_nav_data(nav_data)
        
        result = {
            'returns': self.calculate_returns(processed_nav),
            'risk': self.calculate_risk_metrics(processed_nav),
            'alpha': 0.0,
            'beta': 1.0,
        }
        
        # 如果有基准数据，计算 Alpha/Beta
        if benchmark_data is not None and not benchmark_data.empty:
            alpha, beta = self.calculate_alpha_beta(processed_nav, benchmark_data)
            result['alpha'] = alpha
            result['beta'] = beta
        
        return result


# 创建全局实例
fund_processor = FundDataProcessor()
