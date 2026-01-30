"""
市场新闻采集模块
"""
import re
import requests
from datetime import datetime, date
from typing import List, Dict, Optional
from loguru import logger

from ..models import MarketSummary


class MarketNewsCollector:
    """市场新闻采集器"""
    
    # 东方财富市场数据API
    MARKET_INDEX_API = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    
    # 新闻API
    NEWS_API = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.eastmoney.com/"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def get_market_indices(self) -> Dict[str, dict]:
        """获取主要市场指数行情"""
        try:
            # 指数代码: 1.000001(上证), 0.399001(深证), 1.000300(沪深300)
            params = {
                "fltt": 2,
                "secids": "1.000001,0.399001,1.000300,0.399006,1.000905",
                "fields": "f2,f3,f4,f12,f14"
            }
            
            response = self.session.get(self.MARKET_INDEX_API, params=params, timeout=10)
            data = response.json()
            
            indices = {}
            if data.get("data") and data["data"].get("diff"):
                for item in data["data"]["diff"]:
                    code = item.get("f12", "")
                    indices[code] = {
                        "name": item.get("f14", ""),
                        "price": item.get("f2", 0),
                        "change_pct": item.get("f3", 0) / 100 if item.get("f3") else 0,
                        "change_amt": item.get("f4", 0)
                    }
            
            return indices
            
        except Exception as e:
            logger.error(f"获取市场指数失败: {e}")
            return {}
    
    def get_market_summary(self) -> MarketSummary:
        """获取市场摘要"""
        indices = self.get_market_indices()
        
        sh = indices.get("000001", {})
        sz = indices.get("399001", {})
        hs300 = indices.get("000300", {})
        
        # 判断市场情绪
        avg_change = (sh.get("change_pct", 0) + sz.get("change_pct", 0) + hs300.get("change_pct", 0)) / 3
        if avg_change > 0.02:
            sentiment = "乐观"
        elif avg_change > 0.005:
            sentiment = "偏多"
        elif avg_change > -0.005:
            sentiment = "中性"
        elif avg_change > -0.02:
            sentiment = "偏空"
        else:
            sentiment = "悲观"
        
        return MarketSummary(
            date=date.today(),
            sh_index=sh.get("price", 0),
            sh_change=sh.get("change_pct", 0),
            sz_index=sz.get("price", 0),
            sz_change=sz.get("change_pct", 0),
            hs300_index=hs300.get("price", 0),
            hs300_change=hs300.get("change_pct", 0),
            market_sentiment=sentiment,
            hot_sectors=[],  # 需要额外接口获取
            news_highlights=[]  # 需要额外接口获取
        )
    
    def get_financial_news(self, limit: int = 10) -> List[dict]:
        """获取财经要闻
        
        使用简化的方法，实际项目中可接入更多新闻源
        """
        try:
            # 东方财富财经要闻
            url = "https://np-listapi.eastmoney.com/comm/wap/getListInfo"
            params = {
                "cb": "",
                "client": "wap",
                "type": "5",
                "mession": "1",
                "pageSize": limit,
                "pageIndex": 1,
                "keyword": "",
                "name": "guonei"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            text = response.text
            
            # 简单解析
            news = []
            # 这里简化处理，实际项目需要更复杂的解析
            
            return news
            
        except Exception as e:
            logger.error(f"获取财经新闻失败: {e}")
            return []
    
    def get_hot_sectors(self) -> List[str]:
        """获取热门板块"""
        try:
            # 板块涨幅排行API
            url = "https://push2.eastmoney.com/api/qt/clist/get"
            params = {
                "pn": 1,
                "pz": 10,
                "po": 1,
                "np": 1,
                "fltt": 2,
                "fid": "f3",
                "fs": "m:90+t:2",  # 行业板块
                "fields": "f2,f3,f12,f14"
            }
            
            response = self.session.get(url, params=params, timeout=10)
            data = response.json()
            
            sectors = []
            if data.get("data") and data["data"].get("diff"):
                for item in data["data"]["diff"][:5]:  # 前5个
                    name = item.get("f14", "")
                    change = item.get("f3", 0) / 100
                    if change > 0:
                        sectors.append(f"{name}(+{change*100:.2f}%)")
                    else:
                        sectors.append(f"{name}({change*100:.2f}%)")
            
            return sectors
            
        except Exception as e:
            logger.error(f"获取热门板块失败: {e}")
            return []
    
    def check_market_anomaly(self) -> Optional[dict]:
        """检查市场异常情况
        
        用于熔断判断
        """
        summary = self.get_market_summary()
        
        anomalies = []
        
        # 检查大跌
        if summary.sh_change < -0.03:
            anomalies.append({
                "type": "上证大跌",
                "value": summary.sh_change,
                "severity": "high" if summary.sh_change < -0.05 else "medium"
            })
        
        if summary.hs300_change < -0.03:
            anomalies.append({
                "type": "沪深300大跌",
                "value": summary.hs300_change,
                "severity": "high" if summary.hs300_change < -0.05 else "medium"
            })
        
        if anomalies:
            return {
                "has_anomaly": True,
                "anomalies": anomalies,
                "recommendation": "市场出现异常波动，建议暂停交易操作"
            }
        
        return {
            "has_anomaly": False,
            "anomalies": [],
            "recommendation": "市场正常"
        }


# 创建全局实例
news_collector = MarketNewsCollector()
