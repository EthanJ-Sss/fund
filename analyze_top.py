# -*- coding: utf-8 -*-
"""分析表现优秀的基金"""
import os, sys
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'ignore')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'ignore')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

from src.collector.akshare_collector import AKShareCollector
from src.processor.fund_processor import FundDataProcessor
from src.model.factors import FactorCalculator
from src.model.scoring_model import ScoringModel

# 分析近期表现好的基金
test_funds = [
    ("008528", "华泰柏瑞质量成长A"),
    ("011369", "华商均衡成长混合A"),
    ("001753", "红土创新新兴产业混合A"),
]

collector = AKShareCollector()
processor = FundDataProcessor()
factor_calc = FactorCalculator(processor)
scoring = ScoringModel()

print("\n" + "="*60)
print("[优质基金分析]")
print("="*60)

for fund_code, fund_name in test_funds:
    print(f"\n正在分析: {fund_name} ({fund_code})...")
    
    try:
        nav_data = collector.get_fund_nav_history(fund_code)
        if nav_data is None or nav_data.empty:
            print(f"  [X] 无法获取净值数据")
            continue
        
        print(f"  获取到 {len(nav_data)} 条净值记录")
        
        factors = factor_calc.calculate_all_factors(nav_data=nav_data)
        score_result = scoring.calculate_total_score(factors)
        total_score = score_result['total_score']
        grade = scoring.get_score_grade(total_score)
        
        rec = scoring.get_recommendation(total_score, factors)
        
        print(f"\n  综合评分: {total_score:.1f} 分  等级: {grade}")
        print(f"  投资建议: {rec['action']}")
        print(f"\n  关键指标:")
        print(f"    近1年收益: {factors.get('return_1y', 0):.2f}%")
        print(f"    最大回撤: {factors.get('max_drawdown', 0):.2f}%")
        print(f"    夏普比率: {factors.get('sharpe_ratio', 0):.2f}")
        print(f"    波动率: {factors.get('volatility', 0):.2f}%")
        
        if rec['reasons']:
            print(f"\n  优势:")
            for r in rec['reasons'][:3]:
                print(f"    + {r}")
        
        if rec['risks']:
            print(f"\n  风险:")
            for r in rec['risks'][:3]:
                print(f"    - {r}")
                
    except Exception as e:
        print(f"  [X] 分析失败: {e}")

print("\n" + "="*60)
