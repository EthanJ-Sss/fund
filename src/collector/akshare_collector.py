"""
AKShare 基金数据采集器

使用 AKShare 库获取基金数据，作为主要数据源
"""
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

try:
    import akshare as ak
except ImportError:
    ak = None
    logger.warning("akshare 未安装，部分功能不可用。请运行: pip install akshare")


class AKShareCollector:
    """
    AKShare 基金数据采集器
    """
    
    def __init__(self, request_interval: float = 0.5):
        """
        初始化采集器
        
        Args:
            request_interval: 请求间隔（秒），避免被限流
        """
        if ak is None:
            raise ImportError("akshare 未安装，请运行: pip install akshare")
        
        self.request_interval = request_interval
        self._last_request_time = 0
        self._lock = threading.Lock()
    
    def _rate_limit(self):
        """请求限流（线程安全）"""
        with self._lock:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.request_interval:
                time.sleep(self.request_interval - elapsed)
            self._last_request_time = time.time()
    
    def get_all_funds(self) -> pd.DataFrame:
        """
        获取所有基金列表
        
        Returns:
            DataFrame with columns:
            - 基金代码
            - 基金简称
            - 基金类型
            - 拼音简称
        """
        self._rate_limit()
        try:
            df = ak.fund_name_em()
            logger.info(f"获取基金列表成功，共 {len(df)} 只基金")
            return df
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return pd.DataFrame()
    
    def get_fund_basic_info(self, fund_code: str) -> Dict:
        """
        获取单只基金基本信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Dict containing:
            - 基金全称
            - 基金类型
            - 成立日期
            - 基金规模
            - 基金经理
            - 管理费率
            - 托管费率
            - 业绩比较基准
        """
        self._rate_limit()
        try:
            # 使用雪球接口获取详细信息
            df = ak.fund_individual_basic_info_xq(symbol=fund_code)
            
            # 转换为字典
            info = {}
            for _, row in df.iterrows():
                info[row['item']] = row['value']
            
            return info
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 基本信息失败: {e}")
            return {}
    
    def get_fund_nav_history(
        self, 
        fund_code: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取基金历史净值
        
        Args:
            fund_code: 基金代码
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            DataFrame with columns:
            - 净值日期
            - 单位净值
            - 累计净值
            - 日增长率
        """
        self._rate_limit()
        try:
            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
            
            if df.empty:
                return df
            
            # 转换日期格式
            df['净值日期'] = pd.to_datetime(df['净值日期'])
            
            # 日期过滤
            if start_date:
                df = df[df['净值日期'] >= start_date]
            if end_date:
                df = df[df['净值日期'] <= end_date]
            
            return df.sort_values('净值日期')
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 净值历史失败: {e}")
            return pd.DataFrame()
    
    def get_fund_holdings(self, fund_code: str, year: str = None, quarter: str = None) -> pd.DataFrame:
        """
        获取基金持仓数据
        
        Args:
            fund_code: 基金代码
            year: 年份（默认当年）
            quarter: 季度 (1, 2, 3, 4)（默认最近季度）
            
        Returns:
            DataFrame with columns:
            - 序号
            - 股票代码
            - 股票名称
            - 占净值比例
            - 持股数(万股)
            - 持仓市值(万元)
        """
        self._rate_limit()
        
        # 计算默认年份和季度
        if year is None or quarter is None:
            now = datetime.now()
            year = str(now.year)
            quarter = str((now.month - 1) // 3)
            if quarter == '0':
                year = str(now.year - 1)
                quarter = '4'
        
        try:
            df = ak.fund_portfolio_hold_em(
                symbol=fund_code, 
                date=f"{year}年{quarter}季度"
            )
            return df
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 持仓数据失败: {e}")
            return pd.DataFrame()
    
    def get_index_fund_info(self) -> pd.DataFrame:
        """
        获取所有指数基金信息（含各期涨跌幅）
        
        Returns:
            DataFrame with columns:
            - 基金代码
            - 基金简称
            - 日期
            - 单位净值
            - 累计净值
            - 近1周、近1月、近3月、近6月、近1年涨跌幅
        """
        self._rate_limit()
        try:
            df = ak.fund_info_index_em()
            logger.info(f"获取指数基金列表成功，共 {len(df)} 只")
            return df
        except Exception as e:
            logger.error(f"获取指数基金列表失败: {e}")
            return pd.DataFrame()
    
    def get_fund_scale_history(self, fund_code: str) -> pd.DataFrame:
        """
        获取基金规模变化历史
        
        Args:
            fund_code: 基金代码
            
        Returns:
            DataFrame with scale history
        """
        self._rate_limit()
        try:
            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="规模变动")
            return df
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 规模历史失败: {e}")
            return pd.DataFrame()
    
    def get_fund_manager_info(self, fund_code: str) -> pd.DataFrame:
        """
        获取基金经理信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            DataFrame with manager info
        """
        self._rate_limit()
        try:
            df = ak.fund_manager_em(symbol=fund_code)
            return df
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 经理信息失败: {e}")
            return pd.DataFrame()
    
    def get_fund_rating(self, fund_code: str) -> Dict:
        """
        获取基金评级信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Dict with rating info from various agencies
        """
        self._rate_limit()
        try:
            df = ak.fund_rating_em(symbol=fund_code)
            if df.empty:
                return {}
            
            # 转换为字典
            rating = {}
            for _, row in df.iterrows():
                rating[row.get('评级机构', '')] = row.get('评级', '')
            
            return rating
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 评级失败: {e}")
            return {}
    
    def get_open_fund_rank(self, fund_type: str = "全部") -> pd.DataFrame:
        """
        获取开放式基金排行
        
        Args:
            fund_type: 基金类型（全部/股票型/混合型/债券型/指数型/QDII/FOF）
            
        Returns:
            DataFrame with fund ranking
        """
        self._rate_limit()
        try:
            df = ak.fund_open_fund_rank_em(symbol=fund_type)
            logger.info(f"获取 {fund_type} 基金排行成功，共 {len(df)} 只")
            return df
        except Exception as e:
            logger.error(f"获取 {fund_type} 基金排行失败: {e}")
            return pd.DataFrame()
            
    def get_index_daily(self, symbol: str = "sh000300", start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取指数日行情数据
        
        Args:
            symbol: 指数代码，如 sh000300 (沪深300)
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            
        Returns:
            DataFrame with columns: date, open, close, high, low, volume
        """
        self._rate_limit()
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
            
            # 标准化列名
            if not df.empty:
                # akshare 返回已经是 date, open, high, low, close, volume
                # 转换日期格式
                df['date'] = pd.to_datetime(df['date'])
                
                # 筛选日期
                if start_date:
                    df = df[df['date'] >= pd.to_datetime(start_date)]
                if end_date:
                    df = df[df['date'] <= pd.to_datetime(end_date)]
                    
                df = df.sort_values('date')
            
            return df
        except Exception as e:
            logger.error(f"获取指数数据失败 {symbol}: {e}")
            return pd.DataFrame()


# 创建全局实例
akshare_collector = AKShareCollector() if ak is not None else None
