"""
因子计算模块

基于学术研究的多因子计算：
- 收益因子
- 风险因子  
- 风险调整收益因子
- 规模因子
- 动量因子
- 基金经理因子
- 风格稳定性因子
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from ..processor.fund_processor import FundDataProcessor


class FactorCalculator:
    """
    多因子计算器
    
    支持的因子类别：
    1. 收益因子 (Return Factors)
    2. 风险因子 (Risk Factors)
    3. 风险调整收益因子 (Risk-Adjusted Return Factors)
    4. 规模因子 (Scale Factors)
    5. 动量因子 (Momentum Factors)
    6. 基金经理因子 (Manager Factors)
    7. 风格稳定性因子 (Style Stability Factors)
    """
    
    def __init__(self, processor: FundDataProcessor = None):
        """
        初始化因子计算器
        
        Args:
            processor: 数据处理器实例
        """
        self.processor = processor or FundDataProcessor()
    
    # ============ 收益因子 ============
    
    def calculate_return_factors(
        self, 
        nav_data: pd.DataFrame,
        peer_returns: Dict[str, List[float]] = None
    ) -> Dict[str, float]:
        """
        计算收益因子
        
        Args:
            nav_data: 净值数据
            peer_returns: 同类基金收益率字典 {period: [returns]}
            
        Returns:
            收益因子字典
        """
        processed_nav = self.processor.process_nav_data(nav_data)
        returns = self.processor.calculate_returns(processed_nav)
        
        factors = {}
        
        # 绝对收益因子
        for period, ret in returns.items():
            factors[f'return_{period}'] = ret
        
        # 相对收益因子（排名百分位）
        if peer_returns:
            for period, peers in peer_returns.items():
                if f'return_{period}' in factors:
                    percentile = self.processor.calculate_ranking_percentile(
                        factors[f'return_{period}'], 
                        peers
                    )
                    factors[f'rank_pct_{period}'] = percentile
        
        return factors
    
    # ============ 风险因子 ============
    
    def calculate_risk_factors(self, nav_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算风险因子
        
        Args:
            nav_data: 净值数据
            
        Returns:
            风险因子字典
        """
        processed_nav = self.processor.process_nav_data(nav_data)
        risk_metrics = self.processor.calculate_risk_metrics(processed_nav)
        
        factors = {
            'volatility': risk_metrics.get('volatility', 0),
            'max_drawdown': risk_metrics.get('max_drawdown', 0),
        }
        
        # 计算下行风险
        if not processed_nav.empty and len(processed_nav) >= 30:
            df = processed_nav.copy().sort_values('date')
            df['daily_return'] = df['nav'].pct_change()
            
            negative_returns = df['daily_return'][df['daily_return'] < 0].dropna()
            if len(negative_returns) > 0:
                downside_volatility = negative_returns.std() * np.sqrt(250)
                factors['downside_volatility'] = downside_volatility * 100
            else:
                factors['downside_volatility'] = 0
        
        return factors
    
    # ============ 风险调整收益因子 ============
    
    def calculate_risk_adjusted_factors(
        self, 
        nav_data: pd.DataFrame,
        benchmark_data: pd.DataFrame = None
    ) -> Dict[str, float]:
        """
        计算风险调整收益因子
        
        Args:
            nav_data: 净值数据
            benchmark_data: 基准数据
            
        Returns:
            风险调整收益因子
        """
        processed_nav = self.processor.process_nav_data(nav_data)
        risk_metrics = self.processor.calculate_risk_metrics(processed_nav)
        
        factors = {
            'sharpe_ratio': risk_metrics.get('sharpe_ratio', 0),
            'sortino_ratio': risk_metrics.get('sortino_ratio', 0),
            'calmar_ratio': risk_metrics.get('calmar_ratio', 0),
        }
        
        # 计算信息比率 (IR)
        if benchmark_data is not None and not benchmark_data.empty:
            ir = self._calculate_information_ratio(processed_nav, benchmark_data)
            factors['information_ratio'] = ir
            
            # Alpha 和 Beta
            alpha, beta = self.processor.calculate_alpha_beta(processed_nav, benchmark_data)
            factors['alpha'] = alpha
            factors['beta'] = beta
        
        return factors
    
    def _calculate_information_ratio(
        self, 
        fund_nav: pd.DataFrame, 
        benchmark_nav: pd.DataFrame
    ) -> float:
        """
        计算信息比率
        
        IR = (基金收益 - 基准收益) / 跟踪误差
        """
        if fund_nav.empty or benchmark_nav.empty:
            return 0.0
        
        fund_df = fund_nav.copy().sort_values('date').set_index('date')
        bench_df = benchmark_nav.copy().sort_values('date')
        
        if 'close' in bench_df.columns:
            bench_df['nav'] = bench_df['close']
        bench_df = bench_df.set_index('date')
        
        # 合并数据
        merged = fund_df[['nav']].join(bench_df[['nav']], lsuffix='_fund', rsuffix='_bench')
        merged = merged.dropna()
        
        if len(merged) < 30:
            return 0.0
        
        # 计算收益率
        fund_returns = merged['nav_fund'].pct_change().dropna()
        bench_returns = merged['nav_bench'].pct_change().dropna()
        
        # 超额收益
        excess_returns = fund_returns - bench_returns
        
        # 信息比率
        if excess_returns.std() > 0:
            ir = (excess_returns.mean() * 250) / (excess_returns.std() * np.sqrt(250))
            return ir
        
        return 0.0
    
    # ============ 规模因子 ============
    
    def calculate_scale_factors(
        self, 
        current_scale: float,
        scale_history: List[Tuple[str, float]] = None
    ) -> Dict[str, float]:
        """
        计算规模因子
        
        Args:
            current_scale: 当前规模（亿元）
            scale_history: 规模历史 [(date, scale), ...]
            
        Returns:
            规模因子字典
        """
        factors = {
            'scale': current_scale if current_scale else 0,
        }
        
        # 规模适中性评分（规模太大或太小都不好）
        if current_scale:
            # 理想规模区间：2-100亿
            if 2 <= current_scale <= 100:
                factors['scale_score'] = 100
            elif current_scale < 2:
                # 规模过小，线性降分
                factors['scale_score'] = max(0, current_scale / 2 * 100)
            else:
                # 规模过大，对数降分
                factors['scale_score'] = max(0, 100 - np.log10(current_scale / 100) * 30)
        else:
            factors['scale_score'] = 50
        
        # 规模稳定性
        if scale_history and len(scale_history) >= 2:
            scales = [s[1] for s in scale_history if s[1] and s[1] > 0]
            if len(scales) >= 2:
                scale_changes = [
                    (scales[i] - scales[i-1]) / scales[i-1] 
                    for i in range(1, len(scales))
                ]
                factors['scale_stability'] = 100 - min(100, np.std(scale_changes) * 100)
        
        return factors
    
    # ============ 动量因子 ============
    
    def calculate_momentum_factors(
        self, 
        nav_data: pd.DataFrame,
        lookback_periods: List[int] = None
    ) -> Dict[str, float]:
        """
        计算动量因子
        
        Args:
            nav_data: 净值数据
            lookback_periods: 回看周期（天数）
            
        Returns:
            动量因子字典
        """
        if lookback_periods is None:
            lookback_periods = [20, 60, 120, 250]  # 1个月、3个月、6个月、1年
        
        processed_nav = self.processor.process_nav_data(nav_data)
        
        if processed_nav.empty:
            return {}
        
        df = processed_nav.copy().sort_values('date')
        factors = {}
        
        for period in lookback_periods:
            if len(df) >= period:
                start_nav = df['nav'].iloc[-period]
                end_nav = df['nav'].iloc[-1]
                
                if start_nav > 0:
                    momentum = (end_nav / start_nav - 1) * 100
                    factors[f'momentum_{period}d'] = momentum
        
        # 动量加速度（近期动量 vs 远期动量）
        if 'momentum_20d' in factors and 'momentum_60d' in factors:
            short_momentum = factors['momentum_20d']
            long_momentum = factors['momentum_60d'] / 3  # 归一化到月度
            factors['momentum_acceleration'] = short_momentum - long_momentum
        
        return factors
    
    # ============ 基金经理因子 ============
    
    def calculate_manager_factors(
        self, 
        manager_tenure_years: float,
        manager_total_assets: float = None,
        manager_best_return: float = None,
        manager_funds_count: int = None
    ) -> Dict[str, float]:
        """
        计算基金经理因子
        
        Args:
            manager_tenure_years: 任职年限
            manager_total_assets: 管理总规模（亿元）
            manager_best_return: 最佳回报
            manager_funds_count: 管理基金数量
            
        Returns:
            基金经理因子字典
        """
        factors = {
            'manager_tenure': manager_tenure_years if manager_tenure_years else 0,
        }
        
        # 经验评分（任职年限）
        if manager_tenure_years:
            if manager_tenure_years >= 5:
                factors['manager_experience_score'] = 100
            elif manager_tenure_years >= 3:
                factors['manager_experience_score'] = 80
            elif manager_tenure_years >= 2:
                factors['manager_experience_score'] = 60
            else:
                factors['manager_experience_score'] = manager_tenure_years / 2 * 60
        else:
            factors['manager_experience_score'] = 0
        
        # 管理规模评分
        if manager_total_assets:
            factors['manager_assets'] = manager_total_assets
            # 规模适中性（10-500亿为佳）
            if 10 <= manager_total_assets <= 500:
                factors['manager_scale_score'] = 100
            elif manager_total_assets < 10:
                factors['manager_scale_score'] = manager_total_assets / 10 * 100
            else:
                factors['manager_scale_score'] = max(50, 100 - (manager_total_assets - 500) / 100)
        
        # 管理能力评分（基金数量）
        if manager_funds_count:
            factors['manager_funds_count'] = manager_funds_count
            # 管理1-3只基金最佳
            if 1 <= manager_funds_count <= 3:
                factors['manager_focus_score'] = 100
            elif manager_funds_count <= 5:
                factors['manager_focus_score'] = 80
            else:
                factors['manager_focus_score'] = max(50, 100 - (manager_funds_count - 3) * 10)
        
        return factors
    
    # ============ 风格稳定性因子 ============
    
    def calculate_style_stability(
        self, 
        holdings_history: List[Dict] = None,
        nav_data: pd.DataFrame = None,
        benchmark_data: pd.DataFrame = None
    ) -> Dict[str, float]:
        """
        计算风格稳定性因子（SDS - Style Drift Score）
        
        Args:
            holdings_history: 持仓历史列表
            nav_data: 净值数据
            benchmark_data: 基准数据
            
        Returns:
            风格稳定性因子字典
        """
        factors = {}
        
        # 基于持仓计算风格漂移
        if holdings_history and len(holdings_history) >= 2:
            style_consistency = self._calculate_holding_style_consistency(holdings_history)
            factors['style_consistency'] = style_consistency
        
        # 基于收益率计算风格稳定性
        if nav_data is not None and not nav_data.empty:
            processed_nav = self.processor.process_nav_data(nav_data)
            
            if len(processed_nav) >= 250:  # 至少一年数据
                # 计算滚动 Beta 的稳定性
                if benchmark_data is not None:
                    beta_stability = self._calculate_rolling_beta_stability(
                        processed_nav, benchmark_data
                    )
                    factors['beta_stability'] = beta_stability
                
                # 计算收益稳定性
                return_stability = self._calculate_return_stability(processed_nav)
                factors['return_stability'] = return_stability
        
        # 计算综合风格稳定性得分
        if factors:
            stability_scores = [v for v in factors.values() if not pd.isna(v)]
            if stability_scores:
                factors['style_stability_score'] = np.mean(stability_scores)
        
        return factors
    
    def _calculate_holding_style_consistency(
        self, 
        holdings_history: List[Dict]
    ) -> float:
        """计算持仓风格一致性"""
        if len(holdings_history) < 2:
            return 100
        
        # 提取各期持仓股票代码
        all_holdings = []
        for holdings in holdings_history:
            if isinstance(holdings, dict) and 'stocks' in holdings:
                stocks = set(holdings['stocks'])
            elif isinstance(holdings, pd.DataFrame):
                stocks = set(holdings['股票代码'].tolist() if '股票代码' in holdings.columns else [])
            else:
                continue
            all_holdings.append(stocks)
        
        if len(all_holdings) < 2:
            return 100
        
        # 计算相邻期间的持仓重叠度
        overlaps = []
        for i in range(1, len(all_holdings)):
            prev_stocks = all_holdings[i-1]
            curr_stocks = all_holdings[i]
            
            if prev_stocks and curr_stocks:
                intersection = len(prev_stocks & curr_stocks)
                union = len(prev_stocks | curr_stocks)
                overlap = intersection / union if union > 0 else 0
                overlaps.append(overlap)
        
        return np.mean(overlaps) * 100 if overlaps else 100
    
    def _calculate_rolling_beta_stability(
        self, 
        fund_nav: pd.DataFrame,
        benchmark_nav: pd.DataFrame,
        window: int = 60
    ) -> float:
        """计算滚动 Beta 的稳定性"""
        fund_df = fund_nav.copy().sort_values('date').set_index('date')
        bench_df = benchmark_nav.copy().sort_values('date')
        
        if 'close' in bench_df.columns:
            bench_df['nav'] = bench_df['close']
        bench_df = bench_df.set_index('date')
        
        merged = fund_df[['nav']].join(bench_df[['nav']], lsuffix='_fund', rsuffix='_bench')
        merged = merged.dropna()
        
        if len(merged) < window * 2:
            return 100
        
        fund_returns = merged['nav_fund'].pct_change().dropna()
        bench_returns = merged['nav_bench'].pct_change().dropna()
        
        # 计算滚动 Beta
        betas = []
        for i in range(window, len(fund_returns)):
            f_ret = fund_returns.iloc[i-window:i]
            b_ret = bench_returns.iloc[i-window:i]
            
            cov = np.cov(f_ret, b_ret)[0][1]
            var = np.var(b_ret)
            
            if var > 0:
                beta = cov / var
                betas.append(beta)
        
        if len(betas) < 2:
            return 100
        
        # Beta 稳定性 = 100 - Beta 变化率标准差 * 100
        beta_std = np.std(betas)
        stability = max(0, 100 - beta_std * 100)
        
        return stability
    
    def _calculate_return_stability(self, nav_data: pd.DataFrame) -> float:
        """计算收益稳定性"""
        df = nav_data.copy().sort_values('date')
        
        # 计算月度收益率
        df['month'] = df['date'].dt.to_period('M')
        monthly_nav = df.groupby('month')['nav'].last()
        monthly_returns = monthly_nav.pct_change().dropna()
        
        if len(monthly_returns) < 6:
            return 100
        
        # 计算收益正负比例
        positive_months = (monthly_returns > 0).sum()
        win_rate = positive_months / len(monthly_returns)
        
        # 计算收益稳定性
        return_std = monthly_returns.std()
        stability = max(0, 100 - return_std * 500)  # 波动越大，稳定性越低
        
        # 综合考虑胜率和稳定性
        return stability * 0.7 + win_rate * 100 * 0.3
    
    # ============ 持仓集中度因子 ============
    
    def calculate_concentration_factors(
        self,
        holdings: pd.DataFrame
    ) -> Dict[str, float]:
        """
        计算持仓集中度因子
        
        Args:
            holdings: 持仓数据 DataFrame
            
        Returns:
            集中度因子字典
        """
        factors = {
            'concentration': 0.0
        }
        
        if holdings is None or holdings.empty:
            return factors
            
        # 尝试计算前十大持仓占比
        try:
            # 查找占比列
            ratio_col = None
            for col in ['占净值比例', 'ratio', 'percent']:
                if col in holdings.columns:
                    ratio_col = col
                    break
            
            if ratio_col:
                # 处理百分比字符串
                def parse_ratio(x):
                    if isinstance(x, (int, float)):
                        return float(x)
                    if isinstance(x, str):
                        return float(x.replace('%', ''))
                    return 0.0
                
                ratios = holdings[ratio_col].apply(parse_ratio)
                
                # 计算前10大持仓占比之和
                # 注意：如果数据已经是前10大持仓，直接求和即可
                # 如果是全部持仓，取前10
                top10_concentration = ratios.nlargest(10).sum()
                factors['concentration'] = top10_concentration
        except Exception as e:
            logger.debug(f"计算持仓集中度失败: {e}")
            
        return factors

    # ============ 综合因子计算 ============
    
    def calculate_all_factors(
        self,
        nav_data: pd.DataFrame,
        benchmark_data: pd.DataFrame = None,
        fund_info: Dict = None,
        peer_returns: Dict[str, List[float]] = None,
        holdings_history: List[Dict] = None,
        current_holdings: pd.DataFrame = None
    ) -> Dict[str, float]:
        """
        计算所有因子
        
        Args:
            nav_data: 净值数据
            benchmark_data: 基准数据
            fund_info: 基金基本信息
            peer_returns: 同类基金收益率
            holdings_history: 持仓历史
            current_holdings: 当前持仓数据
            
        Returns:
            所有因子的字典
        """
        all_factors = {}
        
        # 收益因子
        return_factors = self.calculate_return_factors(nav_data, peer_returns)
        all_factors.update(return_factors)
        
        # 风险因子
        risk_factors = self.calculate_risk_factors(nav_data)
        all_factors.update(risk_factors)
        
        # 风险调整收益因子
        risk_adj_factors = self.calculate_risk_adjusted_factors(nav_data, benchmark_data)
        all_factors.update(risk_adj_factors)
        
        # 动量因子
        momentum_factors = self.calculate_momentum_factors(nav_data)
        all_factors.update(momentum_factors)
        
        # 规模因子
        if fund_info:
            current_scale = fund_info.get('scale', 0)
            scale_history = fund_info.get('scale_history', [])
            scale_factors = self.calculate_scale_factors(current_scale, scale_history)
            all_factors.update(scale_factors)
            
            # 基金经理因子
            manager_tenure = fund_info.get('manager_tenure', 0)
            manager_assets = fund_info.get('manager_assets', 0)
            manager_funds = fund_info.get('manager_funds_count', 0)
            manager_factors = self.calculate_manager_factors(
                manager_tenure, manager_assets, None, manager_funds
            )
            all_factors.update(manager_factors)
        
        # 风格稳定性因子
        style_factors = self.calculate_style_stability(
            holdings_history, nav_data, benchmark_data
        )
        all_factors.update(style_factors)
        
        # 持仓集中度因子
        if current_holdings is not None:
            concentration_factors = self.calculate_concentration_factors(current_holdings)
            all_factors.update(concentration_factors)
        
        return all_factors


# 创建全局实例
factor_calculator = FactorCalculator()
