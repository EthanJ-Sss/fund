# -*- coding: utf-8 -*-
"""
量化选基快速演示脚本
"""
import os
import sys

# 设置控制台编码
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'ignore')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'ignore')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loguru import logger
import pandas as pd

# 配置日志输出到控制台
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")


def demo_single_fund_analysis():
    """演示单只基金分析"""
    print("\n" + "="*60)
    print("[单只基金分析演示]")
    print("="*60)
    
    from src.collector.akshare_collector import AKShareCollector
    from src.processor.fund_processor import FundDataProcessor
    from src.model.factors import FactorCalculator
    from src.model.scoring_model import ScoringModel
    from src.model.prefilter import PreFilter
    
    # 分析几只热门基金
    test_funds = [
        ("000001", "华夏成长"),
        ("110011", "易方达中小盘"),
        ("163402", "兴全趋势投资"),
    ]
    
    collector = AKShareCollector()
    processor = FundDataProcessor()
    factor_calc = FactorCalculator(processor)
    scoring = ScoringModel()
    pre_filter = PreFilter()
    
    results = []
    
    for fund_code, fund_name in test_funds:
        print(f"\n正在分析: {fund_name} ({fund_code})...")
        
        try:
            # 获取净值数据
            nav_data = collector.get_fund_nav_history(fund_code)
            
            if nav_data is None or nav_data.empty:
                print(f"  [X] 无法获取净值数据")
                continue
            
            print(f"  获取到 {len(nav_data)} 条净值记录")
            
            # 计算因子
            factors = factor_calc.calculate_all_factors(nav_data=nav_data)
            
            # 评分
            score_result = scoring.calculate_total_score(factors)
            total_score = score_result['total_score']
            grade = scoring.get_score_grade(total_score)
            
            # 预筛选
            prefilter_data = {**factors}
            passed, _ = pre_filter.filter_single(prefilter_data)
            
            results.append({
                'fund_code': fund_code,
                'fund_name': fund_name,
                'total_score': total_score,
                'grade': grade,
                'prefilter_passed': passed,
                'return_1y': factors.get('return_1y', 0),
                'max_drawdown': factors.get('max_drawdown', 0),
                'sharpe_ratio': factors.get('sharpe_ratio', 0),
            })
            
            print(f"  [OK] 评分: {total_score:.1f} 等级: {grade}")
            
        except Exception as e:
            print(f"  [X] 分析失败: {e}")
    
    # 显示结果
    if results:
        print("\n" + "-"*60)
        print("分析结果汇总:")
        print("-"*60)
        
        for r in sorted(results, key=lambda x: x['total_score'], reverse=True):
            grade_icon = {'A': '***', 'B': '**', 'C': '*', 'D': '.', 'E': ' '}.get(r['grade'], ' ')
            passed = '[Y]' if r['prefilter_passed'] else '[N]'
            
            print(f"\n{grade_icon} {r['fund_name']} ({r['fund_code']})")
            print(f"   综合评分: {r['total_score']:.1f}分  等级: {r['grade']}  4433筛选: {passed}")
            print(f"   近1年收益: {r['return_1y']:.2f}%  最大回撤: {r['max_drawdown']:.2f}%  夏普比率: {r['sharpe_ratio']:.2f}")


def demo_fund_list():
    """演示获取基金列表"""
    print("\n" + "="*60)
    print("[基金列表获取演示]")
    print("="*60)
    
    from src.collector.eastmoney_collector import EastMoneyCollector
    from src.collector.alipay_filter import AlipayFundFilter
    
    collector = EastMoneyCollector()
    alipay_filter = AlipayFundFilter()
    
    print("\n正在获取基金列表...")
    fund_list = collector.get_fund_list()
    
    if fund_list:
        print(f"[OK] 获取到 {len(fund_list)} 只基金")
        
        # 转换为 DataFrame 进行过滤
        df = pd.DataFrame(fund_list)
        
        # 过滤支付宝可购基金
        print("\n正在筛选支付宝可购基金...")
        filtered = alipay_filter.filter_purchasable(df)
        print(f"[OK] 支付宝可购: {len(filtered)} 只")
        
        # 分类统计
        categorized = alipay_filter.categorize_funds(filtered)
        print("\n按类型分类:")
        for fund_type, funds in categorized.items():
            if not funds.empty:
                print(f"  {fund_type}: {len(funds)} 只")
    else:
        print("[X] 获取基金列表失败")


def demo_top_funds():
    """演示获取业绩排名"""
    print("\n" + "="*60)
    print("[基金业绩排名演示]")
    print("="*60)
    
    from src.collector.eastmoney_collector import EastMoneyCollector
    
    collector = EastMoneyCollector()
    
    print("\n正在获取股票型基金排名...")
    rank_df = collector.get_fund_performance_rank(fund_type='gp', page_size=20)
    
    if not rank_df.empty:
        print(f"\n股票型基金 近1年收益 TOP 10:")
        print("-"*50)
        
        # 按近1年收益排序
        if 'return_1y' in rank_df.columns:
            top10 = rank_df.nlargest(10, 'return_1y')
        else:
            top10 = rank_df.head(10)
        
        for i, row in enumerate(top10.itertuples(), 1):
            code = getattr(row, 'fund_code', '')
            name = getattr(row, 'fund_name', '')[:12]
            ret_1y = getattr(row, 'return_1y', 0)
            
            if pd.notna(ret_1y):
                print(f"  {i:2d}. {name:12s} ({code})  {ret_1y:+.2f}%")
    else:
        print("[X] 获取排名失败")


def main():
    print("""
================================================================
              量化选基系统 - 快速演示
================================================================
    """)
    
    try:
        # 1. 基金列表演示
        demo_fund_list()
        
        # 2. 业绩排名演示
        demo_top_funds()
        
        # 3. 单只基金分析演示
        demo_single_fund_analysis()
        
        print("\n" + "="*60)
        print("[OK] 演示完成!")
        print("="*60)
        print("\n提示: 使用 main.py 的交互命令进行更多操作:")
        print("  > screen        运行完整量化选基分析")
        print("  > top 股票型 10  查看推荐基金 TOP 10")
        print("  > detail 000001  查看基金详细分析")
        print()
        
    except Exception as e:
        print(f"\n[X] 演示过程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
