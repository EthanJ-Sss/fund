"""
基金数据存储模块

提供本地文件存储和缓存功能
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from loguru import logger


class FundStorage:
    """
    基金数据存储器
    
    功能：
    1. 基金列表缓存
    2. 基金详情数据存储
    3. 评分结果存储
    4. 历史数据管理
    """
    
    def __init__(self, base_path: str = None):
        """
        初始化存储器
        
        Args:
            base_path: 数据存储根目录
        """
        if base_path is None:
            # 默认存储在项目根目录的 data 文件夹
            base_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
        
        self.base_path = Path(base_path)
        self._init_directories()
    
    def _init_directories(self):
        """初始化存储目录结构"""
        directories = [
            self.base_path,
            self.base_path / 'funds',           # 基金基本信息
            self.base_path / 'nav',             # 净值数据
            self.base_path / 'holdings',        # 持仓数据
            self.base_path / 'scores',          # 评分结果
            self.base_path / 'cache',           # 临时缓存
            self.base_path / 'reports',         # 分析报告
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    # ============ 基金列表存储 ============
    
    def save_fund_list(
        self, 
        funds: Union[pd.DataFrame, List[Dict]], 
        fund_type: str = 'all'
    ):
        """
        保存基金列表
        
        Args:
            funds: 基金列表 DataFrame 或 字典列表
            fund_type: 基金类型标识
        """
        filename = f'fund_list_{fund_type}.json'
        filepath = self.base_path / 'cache' / filename
        
        if isinstance(funds, pd.DataFrame):
            data = funds.to_dict('records')
        else:
            data = funds
        
        save_data = {
            'update_time': datetime.now().isoformat(),
            'count': len(data),
            'funds': data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"保存基金列表: {filename}, 共 {len(data)} 只基金")
    
    def load_fund_list(
        self, 
        fund_type: str = 'all',
        max_age_hours: int = 24
    ) -> Optional[pd.DataFrame]:
        """
        加载基金列表（带缓存过期检查）
        
        Args:
            fund_type: 基金类型标识
            max_age_hours: 最大缓存时间（小时）
            
        Returns:
            基金列表 DataFrame 或 None
        """
        filename = f'fund_list_{fund_type}.json'
        filepath = self.base_path / 'cache' / filename
        
        if not filepath.exists():
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查缓存是否过期
            update_time = datetime.fromisoformat(data['update_time'])
            if datetime.now() - update_time > timedelta(hours=max_age_hours):
                logger.info(f"基金列表缓存已过期: {filename}")
                return None
            
            logger.info(f"加载基金列表缓存: {filename}, 共 {data['count']} 只基金")
            return pd.DataFrame(data['funds'])
            
        except Exception as e:
            logger.error(f"加载基金列表失败: {e}")
            return None
    
    # ============ 基金详情存储 ============
    
    def save_fund_info(self, fund_code: str, info: Dict):
        """
        保存基金详情
        
        Args:
            fund_code: 基金代码
            info: 基金信息字典
        """
        filepath = self.base_path / 'funds' / f'{fund_code}.json'
        
        save_data = {
            'fund_code': fund_code,
            'update_time': datetime.now().isoformat(),
            **info
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
    
    def load_fund_info(
        self, 
        fund_code: str,
        max_age_hours: int = 168  # 7天
    ) -> Optional[Dict]:
        """
        加载基金详情
        
        Args:
            fund_code: 基金代码
            max_age_hours: 最大缓存时间
            
        Returns:
            基金信息字典或 None
        """
        filepath = self.base_path / 'funds' / f'{fund_code}.json'
        
        if not filepath.exists():
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查缓存是否过期
            update_time = datetime.fromisoformat(data['update_time'])
            if datetime.now() - update_time > timedelta(hours=max_age_hours):
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"加载基金详情失败 {fund_code}: {e}")
            return None
    
    # ============ 净值数据存储 ============
    
    def save_nav_data(self, fund_code: str, nav_data: pd.DataFrame):
        """
        保存净值数据
        
        Args:
            fund_code: 基金代码
            nav_data: 净值数据 DataFrame
        """
        filepath = self.base_path / 'nav' / f'{fund_code}.csv'
        nav_data.to_csv(filepath, index=False, encoding='utf-8')
    
    def load_nav_data(
        self, 
        fund_code: str,
        max_age_hours: int = 24
    ) -> Optional[pd.DataFrame]:
        """
        加载净值数据
        
        Args:
            fund_code: 基金代码
            max_age_hours: 最大缓存时间
            
        Returns:
            净值数据 DataFrame 或 None
        """
        filepath = self.base_path / 'nav' / f'{fund_code}.csv'
        
        if not filepath.exists():
            return None
        
        # 检查文件修改时间
        mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
        if datetime.now() - mtime > timedelta(hours=max_age_hours):
            return None
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            logger.error(f"加载净值数据失败 {fund_code}: {e}")
            return None
    
    # ============ 持仓数据存储 ============
    
    def save_holdings(
        self, 
        fund_code: str, 
        holdings: pd.DataFrame,
        period: str = None
    ):
        """
        保存持仓数据
        
        Args:
            fund_code: 基金代码
            holdings: 持仓数据
            period: 报告期（如 2024Q1）
        """
        if period:
            filename = f'{fund_code}_{period}.csv'
        else:
            filename = f'{fund_code}_latest.csv'
        
        filepath = self.base_path / 'holdings' / filename
        holdings.to_csv(filepath, index=False, encoding='utf-8')
    
    def load_holdings(
        self, 
        fund_code: str,
        period: str = None
    ) -> Optional[pd.DataFrame]:
        """
        加载持仓数据
        
        Args:
            fund_code: 基金代码
            period: 报告期
            
        Returns:
            持仓数据 DataFrame 或 None
        """
        if period:
            filename = f'{fund_code}_{period}.csv'
        else:
            filename = f'{fund_code}_latest.csv'
        
        filepath = self.base_path / 'holdings' / filename
        
        if not filepath.exists():
            return None
        
        try:
            return pd.read_csv(filepath, encoding='utf-8')
        except Exception as e:
            logger.error(f"加载持仓数据失败 {fund_code}: {e}")
            return None
    
    # ============ 评分结果存储 ============
    
    def save_scores(
        self, 
        scores: Union[pd.DataFrame, List[Dict]],
        fund_type: str = 'all',
        date: str = None
    ):
        """
        保存评分结果
        
        Args:
            scores: 评分结果
            fund_type: 基金类型
            date: 评分日期
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        filename = f'scores_{fund_type}_{date}.json'
        filepath = self.base_path / 'scores' / filename
        
        if isinstance(scores, pd.DataFrame):
            data = scores.to_dict('records')
        else:
            data = scores
        
        save_data = {
            'date': date,
            'fund_type': fund_type,
            'update_time': datetime.now().isoformat(),
            'count': len(data),
            'scores': data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
        
        # 同时保存为最新版本
        latest_filepath = self.base_path / 'scores' / f'scores_{fund_type}_latest.json'
        with open(latest_filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"保存评分结果: {filename}, 共 {len(data)} 只基金")
    
    def load_scores(
        self, 
        fund_type: str = 'all',
        date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        加载评分结果
        
        Args:
            fund_type: 基金类型
            date: 评分日期，None 表示最新
            
        Returns:
            评分结果 DataFrame 或 None
        """
        if date:
            filename = f'scores_{fund_type}_{date}.json'
        else:
            filename = f'scores_{fund_type}_latest.json'
        
        filepath = self.base_path / 'scores' / filename
        
        if not filepath.exists():
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"加载评分结果: {filename}, 共 {data['count']} 只基金")
            return pd.DataFrame(data['scores'])
            
        except Exception as e:
            logger.error(f"加载评分结果失败: {e}")
            return None
    
    def get_top_funds(
        self, 
        fund_type: str = 'all',
        top_n: int = 20,
        min_score: float = 60
    ) -> pd.DataFrame:
        """
        获取评分最高的基金
        
        Args:
            fund_type: 基金类型
            top_n: 返回数量
            min_score: 最低评分
            
        Returns:
            排名前 N 的基金 DataFrame
        """
        scores_df = self.load_scores(fund_type)
        
        if scores_df is None or scores_df.empty:
            return pd.DataFrame()
        
        # 筛选并排序
        filtered = scores_df[scores_df['total_score'] >= min_score]
        sorted_df = filtered.sort_values('total_score', ascending=False)
        
        return sorted_df.head(top_n)
    
    # ============ 报告存储 ============
    
    def save_report(
        self, 
        report: Dict,
        report_type: str = 'analysis',
        date: str = None
    ):
        """
        保存分析报告
        
        Args:
            report: 报告内容
            report_type: 报告类型
            date: 报告日期
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        filename = f'{report_type}_{date}.json'
        filepath = self.base_path / 'reports' / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"保存报告: {filename}")
    
    def load_latest_report(
        self, 
        report_type: str = 'analysis'
    ) -> Optional[Dict]:
        """
        加载最新报告
        
        Args:
            report_type: 报告类型
            
        Returns:
            报告内容或 None
        """
        report_dir = self.base_path / 'reports'
        reports = list(report_dir.glob(f'{report_type}_*.json'))
        
        if not reports:
            return None
        
        # 按修改时间排序，取最新
        latest = max(reports, key=lambda p: p.stat().st_mtime)
        
        try:
            with open(latest, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载报告失败: {e}")
            return None
    
    # ============ 缓存管理 ============
    
    def clear_cache(self, older_than_days: int = 7):
        """
        清理过期缓存
        
        Args:
            older_than_days: 清理多少天前的缓存
        """
        cache_dir = self.base_path / 'cache'
        threshold = datetime.now() - timedelta(days=older_than_days)
        
        cleared = 0
        for filepath in cache_dir.iterdir():
            if filepath.is_file():
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
                if mtime < threshold:
                    filepath.unlink()
                    cleared += 1
        
        logger.info(f"清理缓存: 删除 {cleared} 个过期文件")
    
    def get_storage_stats(self) -> Dict:
        """
        获取存储统计信息
        
        Returns:
            存储统计字典
        """
        stats = {
            'base_path': str(self.base_path),
            'directories': {}
        }
        
        for directory in self.base_path.iterdir():
            if directory.is_dir():
                files = list(directory.iterdir())
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                stats['directories'][directory.name] = {
                    'file_count': len(files),
                    'total_size_mb': round(total_size / 1024 / 1024, 2)
                }
        
        return stats


# 创建全局实例
fund_storage = FundStorage()
