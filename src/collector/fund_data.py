"""
基金数据采集模块
"""
import re
import json
import requests
from datetime import date, datetime
from typing import Optional, List, Dict
from loguru import logger

from ..models import FundNav, FundInfo, FundType


class FundDataCollector:
    """基金数据采集器"""
    
    # 天天基金API
    FUND_NAV_API = "http://fundgz.1234567.com.cn/js/{fund_code}.js"
    FUND_DETAIL_API = "http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
    FUND_NET_VALUE_API = "http://api.fund.eastmoney.com/f10/lsjz"
    FUND_INFO_API = "http://fund.eastmoney.com/{fund_code}.html"
    
    # 请求头
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://fund.eastmoney.com/"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def get_fund_estimate(self, fund_code: str) -> Optional[FundNav]:
        """获取基金实时估值
        
        注意：货币基金没有估值数据
        """
        try:
            url = self.FUND_NAV_API.format(fund_code=fund_code)
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            
            # 解析JSONP格式: jsonpgz({...});
            text = response.text
            match = re.search(r'jsonpgz\((.*?)\);', text)
            if not match:
                logger.warning(f"基金 {fund_code} 估值数据解析失败")
                return None
            
            data = json.loads(match.group(1))
            
            return FundNav(
                code=data.get('fundcode', fund_code),
                name=data.get('name', ''),
                nav=float(data.get('dwjz', 0)),  # 单位净值
                estimate_nav=float(data.get('gsz', 0)),  # 估算净值
                estimate_return=float(data.get('gszzl', 0)) / 100,  # 估算涨跌幅
                nav_date=date.today()
            )
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 估值失败: {e}")
            return None
    
    def get_fund_nav_history(
        self, 
        fund_code: str, 
        start_date: str = None,
        end_date: str = None,
        page_size: int = 20
    ) -> List[dict]:
        """获取基金历史净值
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            page_size: 每页条数
        """
        try:
            params = {
                "fundCode": fund_code,
                "pageIndex": 1,
                "pageSize": page_size,
                "startDate": start_date or "",
                "endDate": end_date or "",
            }
            
            response = self.session.get(
                self.FUND_NET_VALUE_API,
                params=params,
                timeout=10
            )
            data = response.json()
            
            if data.get("ErrCode") == 0:
                nav_list = data.get("Data", {}).get("LSJZList", [])
                return [
                    {
                        "date": item.get("FSRQ"),
                        "nav": float(item.get("DWJZ", 0)),
                        "acc_nav": float(item.get("LJJZ", 0)),
                        "daily_return": float(item.get("JZZZL", 0)) / 100 if item.get("JZZZL") else 0
                    }
                    for item in nav_list
                ]
            return []
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 历史净值失败: {e}")
            return []
    
    def get_fund_detail(self, fund_code: str) -> Optional[dict]:
        """获取基金详细信息
        
        包含基金经理、业绩走势、持仓等
        """
        try:
            url = self.FUND_DETAIL_API.format(fund_code=fund_code)
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            text = response.text
            
            result = {}
            
            # 解析基金经理信息
            manager_match = re.search(r'var Data_currentFundManager\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if manager_match:
                managers = json.loads(manager_match.group(1).rstrip(';'))
                result['managers'] = [
                    {
                        'name': m.get('name'),
                        'work_days': m.get('workTime'),
                        'fund_size': m.get('fundSize')
                    }
                    for m in managers
                ]
            
            # 解析业绩走势数据
            trend_match = re.search(r'var Data_netWorthTrend\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if trend_match:
                # 只取最近的数据点
                trends = json.loads(trend_match.group(1).rstrip(';'))
                if trends:
                    result['recent_nav'] = {
                        'date': datetime.fromtimestamp(trends[-1]['x'] / 1000).strftime('%Y-%m-%d'),
                        'nav': trends[-1]['y']
                    }
            
            # 解析同类排名
            rank_match = re.search(r'var Data_rateInSimilarType\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if rank_match:
                ranks = json.loads(rank_match.group(1).rstrip(';'))
                result['rank_in_similar'] = ranks
            
            # 解析持仓股票
            stocks_match = re.search(r'var stockCodesNew\s*=\s*"(.*?)";', text)
            if stocks_match:
                result['top_stocks'] = stocks_match.group(1).split(',') if stocks_match.group(1) else []
            
            return result
            
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 详情失败: {e}")
            return None
    
    def get_fund_performance(self, fund_code: str) -> Optional[dict]:
        """获取基金业绩表现
        
        返回各时间段收益率
        """
        try:
            url = self.FUND_DETAIL_API.format(fund_code=fund_code)
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            text = response.text
            
            # 解析业绩数据 (类似: var syl_1n = "15.23";)
            performance = {}
            
            patterns = {
                'day': r'var syl_1d\s*=\s*"(.*?)";',
                'week': r'var syl_1z\s*=\s*"(.*?)";',
                'month_1': r'var syl_1y\s*=\s*"(.*?)";',
                'month_3': r'var syl_3y\s*=\s*"(.*?)";',
                'month_6': r'var syl_6y\s*=\s*"(.*?)";',
                'year_1': r'var syl_1n\s*=\s*"(.*?)";',
                'year_2': r'var syl_2n\s*=\s*"(.*?)";',
                'year_3': r'var syl_3n\s*=\s*"(.*?)";',
                'since_establish': r'var syl_lnz\s*=\s*"(.*?)";',
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, text)
                if match and match.group(1):
                    try:
                        performance[key] = float(match.group(1)) / 100
                    except ValueError:
                        performance[key] = None
                else:
                    performance[key] = None
            
            return performance
            
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 业绩失败: {e}")
            return None
    
    def batch_get_nav(self, fund_codes: List[str]) -> Dict[str, FundNav]:
        """批量获取基金净值"""
        result = {}
        for code in fund_codes:
            nav = self.get_fund_estimate(code)
            if nav:
                result[code] = nav
        return result
    
    def detect_fund_type(self, fund_code: str, fund_name: str = "") -> FundType:
        """检测基金类型
        
        基于基金名称和代码简单判断
        """
        name = fund_name.lower()
        
        if '货币' in name or '现金' in name:
            return FundType.MONEY
        elif '债' in name or '利率' in name or '信用' in name:
            return FundType.BOND
        elif '指数' in name or 'ETF' in name.upper() or '联接' in name:
            return FundType.INDEX
        elif 'QDII' in name.upper() or '海外' in name or '美国' in name or '港股' in name:
            return FundType.QDII
        elif '股票' in name or '成长' in name or '价值' in name:
            return FundType.STOCK
        elif '混合' in name or '平衡' in name or '配置' in name or '灵活' in name:
            return FundType.HYBRID
        else:
            # 默认根据代码判断
            if fund_code.startswith('5') or fund_code.startswith('1'):
                return FundType.INDEX  # ETF
            return FundType.HYBRID  # 默认混合


# 创建全局实例
fund_collector = FundDataCollector()
