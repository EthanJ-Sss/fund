"""
天天基金网数据采集器

提供天天基金实时数据和业绩排名等功能
"""
import json
import re
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from loguru import logger


class EastMoneyCollector:
    """
    天天基金网数据采集器
    
    主要接口：
    - 基金列表: fund.eastmoney.com/js/fundcode_search.js
    - 实时估值: fundgz.1234567.com.cn/js/{code}.js
    - 详细数据: fund.eastmoney.com/pingzhongdata/{code}.js
    - 业绩排名: fund.eastmoney.com/data/rankhandler.aspx
    """
    
    BASE_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://fund.eastmoney.com/',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }
    
    def __init__(self, request_interval: float = 0.3):
        """
        初始化采集器
        
        Args:
            request_interval: 请求间隔（秒），避免被限流
        """
        self.request_interval = request_interval
        self._last_request_time = 0
        self.session = requests.Session()
        self.session.headers.update(self.BASE_HEADERS)
    
    def _rate_limit(self):
        """请求限流"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_interval:
            time.sleep(self.request_interval - elapsed)
        self._last_request_time = time.time()
    
    def get_fund_list(self) -> List[Dict]:
        """
        获取所有基金列表
        
        Returns:
            List of dicts:
            - code: 基金代码
            - abbr: 拼音简称
            - name: 基金名称
            - type: 基金类型
            - pinyin: 全拼
        """
        self._rate_limit()
        url = "http://fund.eastmoney.com/js/fundcode_search.js"
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            
            # 解析 JavaScript 变量
            # 格式: var r = [["000001","HXCZHH","华夏成长混合","混合型-灵活","HUAXIACHENGZHANGHUNHE"],...]
            content = response.text
            match = re.search(r'var r = (\[.*?\]);', content, re.S)
            
            if match:
                data = json.loads(match.group(1))
                funds = []
                for item in data:
                    funds.append({
                        'code': item[0],
                        'abbr': item[1],
                        'name': item[2],
                        'type': item[3],
                        'pinyin': item[4]
                    })
                logger.info(f"获取基金列表成功，共 {len(funds)} 只")
                return funds
            
            return []
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return []
    
    def get_fund_realtime(self, fund_code: str) -> Optional[Dict]:
        """
        获取基金实时估值数据
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Dict containing:
            - fundcode: 基金代码
            - name: 基金名称
            - jzrq: 净值日期
            - dwjz: 单位净值（上一交易日）
            - gsz: 估算净值（实时）
            - gszzl: 估算涨跌幅（百分比）
            - gztime: 估算时间
        """
        self._rate_limit()
        url = f"http://fundgz.1234567.com.cn/js/{fund_code}.js"
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            
            # 解析 JSONP
            # 格式: jsonpgz({"fundcode":"000001","name":"华夏成长混合",...});
            content = response.text
            match = re.search(r'jsonpgz\((.*)\)', content)
            
            if match:
                data = json.loads(match.group(1))
                return data
            
            return None
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 实时数据失败: {e}")
            return None
    
    def get_fund_detail(self, fund_code: str) -> Dict:
        """
        获取基金详细数据（包括历史净值、持仓等）
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Dict containing various fund data
        """
        self._rate_limit()
        url = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            content = response.text
            
            data = {}
            
            # 解析各个变量
            patterns = {
                'fS_name': r'var fS_name = "(.+?)";',  # 基金名称
                'fS_code': r'var fS_code = "(.+?)";',  # 基金代码
                'fund_sourceRate': r'var fund_sourceRate="(.+?)";',  # 原申购费率
                'fund_Rate': r'var fund_Rate="(.+?)";',  # 现申购费率
                'fund_minsg': r'var fund_minsg="(.+?)";',  # 最小申购金额
                'syl_1n': r'var syl_1n="(.+?)";',  # 近1年收益率
                'syl_6y': r'var syl_6y="(.+?)";',  # 近6月收益率
                'syl_3y': r'var syl_3y="(.+?)";',  # 近3月收益率
                'syl_1y': r'var syl_1y="(.+?)";',  # 近1月收益率
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, content)
                if match:
                    value = match.group(1)
                    try:
                        data[key] = json.loads(value) if value.startswith('[') else value
                    except:
                        data[key] = value
            
            # 解析复杂数据结构
            # 历史净值数据
            nav_pattern = r'var Data_netWorthTrend = (\[.*?\]);'
            match = re.search(nav_pattern, content, re.S)
            if match:
                try:
                    data['nav_history'] = json.loads(match.group(1))
                except:
                    pass
            
            # 持仓数据
            holdings_pattern = r'var stockCodesNew=(\[.*?\]);'
            match = re.search(holdings_pattern, content, re.S)
            if match:
                try:
                    data['holdings'] = json.loads(match.group(1))
                except:
                    pass
            
            # 基金经理信息
            manager_pattern = r'var Data_currentFundManager=(\[.*?\]);'
            match = re.search(manager_pattern, content, re.S)
            if match:
                try:
                    data['managers'] = json.loads(match.group(1))
                except:
                    pass
            
            return data
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 详细数据失败: {e}")
            return {}
    
    def get_fund_performance_rank(self, fund_type: str = 'all', page_size: int = 500) -> pd.DataFrame:
        """
        获取基金业绩排名
        
        Args:
            fund_type: 基金类型 (all, stock, mixed, bond, index)
            page_size: 每页数量
            
        Returns:
            DataFrame with fund performance data
        """
        self._rate_limit()
        
        type_map = {
            'all': '',
            'stock': 'gp',
            'mixed': 'hh',
            'bond': 'zq',
            'index': 'zs'
        }
        
        ft = type_map.get(fund_type, '')
        
        url = "http://fund.eastmoney.com/data/rankhandler.aspx"
        params = {
            'op': 'ph',
            'dt': 'kf',
            'ft': ft,
            'rs': '',
            'gs': '0',
            'sc': '1nzf',
            'st': 'desc',
            'sd': '',
            'ed': '',
            'qdii': '',
            'tabSubtype': ',,,,,',
            'pi': '1',
            'pn': str(page_size),
            'dx': '1',
            'v': '0.123456'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.encoding = 'utf-8'
            
            # 解析返回数据
            content = response.text
            
            # 尝试提取 datas 数组（直接提取字符串数组）
            # 格式: var rankData = {datas:["000001,华夏成长,...",...],allRecords:...}
            match = re.search(r'datas:\[(.*?)\],', content, re.S)
            
            if not match:
                logger.warning("无法解析基金排名数据格式")
                return pd.DataFrame()
            
            datas_str = match.group(1)
            
            # 提取每个基金数据项
            items = re.findall(r'"([^"]*)"', datas_str)
            
            if not items:
                return pd.DataFrame()
            
            funds_data = []
            for item in items:
                parts = item.split(',')
                if len(parts) >= 15:
                    funds_data.append({
                        'fund_code': parts[0],
                        'fund_name': parts[1],
                        'nav_date': parts[3] if len(parts) > 3 else '',
                        'nav': self._safe_float(parts[4]) if len(parts) > 4 else None,
                        'acc_nav': self._safe_float(parts[5]) if len(parts) > 5 else None,
                        'return_1d': self._safe_float(parts[6]) if len(parts) > 6 else None,
                        'return_1w': self._safe_float(parts[7]) if len(parts) > 7 else None,
                        'return_1m': self._safe_float(parts[8]) if len(parts) > 8 else None,
                        'return_3m': self._safe_float(parts[9]) if len(parts) > 9 else None,
                        'return_6m': self._safe_float(parts[10]) if len(parts) > 10 else None,
                        'return_1y': self._safe_float(parts[11]) if len(parts) > 11 else None,
                        'return_2y': self._safe_float(parts[12]) if len(parts) > 12 else None,
                        'return_3y': self._safe_float(parts[13]) if len(parts) > 13 else None,
                        'return_ytd': self._safe_float(parts[14]) if len(parts) > 14 else None,
                    })
            
            df = pd.DataFrame(funds_data)
            logger.info(f"获取 {fund_type} 基金排名成功，共 {len(df)} 只")
            return df
            
        except Exception as e:
            logger.error(f"获取基金排名失败: {e}")
            return pd.DataFrame()
    
    def _safe_float(self, value: str) -> Optional[float]:
        """安全转换为浮点数"""
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None
    
    def batch_get_realtime(self, fund_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取基金实时数据
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            Dict[fund_code, realtime_data]
        """
        results = {}
        for code in fund_codes:
            data = self.get_fund_realtime(code)
            if data:
                results[code] = data
        return results


# 创建全局实例
eastmoney_collector = EastMoneyCollector()
