"""
指数估值数据采集模块
"""
import requests
from datetime import date
from typing import Optional, List, Dict
from loguru import logger

from ..models import IndexValuation


class IndexValuationCollector:
    """指数估值数据采集器"""
    
    # 常用指数代码映射
    INDEX_CODES = {
        "000300": "沪深300",
        "000905": "中证500",
        "000852": "中证1000",
        "399006": "创业板指",
        "000016": "上证50",
        "399673": "创业板50",
        "000922": "中证红利",
        "000015": "红利指数",
        "399324": "深证红利",
        "HSI": "恒生指数",
        "HSCEI": "恒生国企",
        "HSTECH": "恒生科技",
        "000991": "全指医药",
        "000932": "中证消费",
        "930758": "中证消费50",
        "399967": "中证军工",
        "000827": "中证环保",
        "399986": "中证银行",
        "399975": "证券公司",
        "399808": "中证新能",
        "931071": "中证畜牧",
    }
    
    # 蛋卷估值API（非官方，可能变化）
    DANJUAN_API = "https://danjuanfunds.com/djapi/index_eva/dj"
    
    # 备用：自定义历史数据计算
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://danjuanfunds.com/"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        # 缓存估值数据
        self._valuation_cache: Dict[str, IndexValuation] = {}
        self._cache_date: Optional[date] = None
    
    def get_all_valuations(self) -> List[IndexValuation]:
        """获取所有主要指数的估值数据"""
        # 如果今日已缓存，直接返回
        if self._cache_date == date.today() and self._valuation_cache:
            return list(self._valuation_cache.values())
        
        try:
            response = self.session.get(self.DANJUAN_API, timeout=10)
            data = response.json()
            
            if data.get("result_code") == 0:
                items = data.get("data", {}).get("items", [])
                valuations = []
                
                for item in items:
                    try:
                        valuation = IndexValuation(
                            index_code=item.get("index_code", ""),
                            index_name=item.get("name", ""),
                            pe=float(item.get("pe", 0)),
                            pe_percentile=float(item.get("pe_percentile", 50)),
                            pb=float(item.get("pb", 0)),
                            pb_percentile=float(item.get("pb_percentile", 50)),
                            dividend_yield=float(item.get("yeild", 0)) if item.get("yeild") else None,
                            update_date=date.today()
                        )
                        valuations.append(valuation)
                        self._valuation_cache[valuation.index_code] = valuation
                    except Exception as e:
                        logger.warning(f"解析指数估值数据失败: {e}")
                        continue
                
                self._cache_date = date.today()
                logger.info(f"成功获取 {len(valuations)} 个指数估值数据")
                return valuations
            else:
                logger.warning("获取指数估值数据失败，使用默认估值")
                return self._get_default_valuations()
                
        except Exception as e:
            logger.error(f"获取指数估值失败: {e}")
            return self._get_default_valuations()
    
    def get_valuation(self, index_code: str) -> Optional[IndexValuation]:
        """获取单个指数的估值"""
        # 确保缓存已更新
        if self._cache_date != date.today():
            self.get_all_valuations()
        
        return self._valuation_cache.get(index_code)
    
    def get_hs300_valuation(self) -> Optional[IndexValuation]:
        """获取沪深300估值（常用）"""
        return self.get_valuation("000300")
    
    def get_market_overall_valuation(self) -> dict:
        """获取市场整体估值水平
        
        基于沪深300、中证500的综合判断
        """
        self.get_all_valuations()
        
        hs300 = self._valuation_cache.get("000300")
        zz500 = self._valuation_cache.get("000905")
        
        if not hs300 and not zz500:
            return {
                "level": "正常",
                "pe_percentile": 50,
                "suggestion": "市场估值数据获取失败，建议谨慎操作"
            }
        
        # 综合PE百分位
        percentiles = []
        if hs300:
            percentiles.append(hs300.pe_percentile)
        if zz500:
            percentiles.append(zz500.pe_percentile)
        
        avg_percentile = sum(percentiles) / len(percentiles)
        
        if avg_percentile < 20:
            level = "极度低估"
            suggestion = "市场处于历史低位，是长期布局的好时机"
        elif avg_percentile < 40:
            level = "低估"
            suggestion = "市场估值偏低，可以逐步建仓"
        elif avg_percentile < 60:
            level = "正常"
            suggestion = "市场估值适中，维持正常配置"
        elif avg_percentile < 80:
            level = "高估"
            suggestion = "市场估值偏高，控制仓位，谨慎加仓"
        else:
            level = "极度高估"
            suggestion = "市场处于历史高位，建议降低仓位，注意风险"
        
        return {
            "level": level,
            "pe_percentile": avg_percentile,
            "suggestion": suggestion,
            "details": {
                "hs300": hs300.model_dump() if hs300 else None,
                "zz500": zz500.model_dump() if zz500 else None
            }
        }
    
    def _get_default_valuations(self) -> List[IndexValuation]:
        """返回默认估值数据（当API失败时）"""
        # 提供一些保守的默认值
        defaults = [
            ("000300", "沪深300", 12.5, 50, 1.4, 50, 2.5),
            ("000905", "中证500", 22.0, 50, 1.8, 50, 1.5),
            ("399006", "创业板指", 35.0, 50, 4.5, 50, 0.5),
        ]
        
        valuations = []
        for code, name, pe, pe_pct, pb, pb_pct, div in defaults:
            valuations.append(IndexValuation(
                index_code=code,
                index_name=name,
                pe=pe,
                pe_percentile=pe_pct,
                pb=pb,
                pb_percentile=pb_pct,
                dividend_yield=div,
                update_date=date.today()
            ))
        
        return valuations
    
    def get_sector_valuations(self) -> Dict[str, IndexValuation]:
        """获取行业指数估值"""
        self.get_all_valuations()
        
        sector_codes = [
            "000991",  # 医药
            "000932",  # 消费
            "399967",  # 军工
            "399986",  # 银行
            "399975",  # 证券
            "399808",  # 新能源
        ]
        
        return {
            code: self._valuation_cache[code]
            for code in sector_codes
            if code in self._valuation_cache
        }


# 创建全局实例
valuation_collector = IndexValuationCollector()
