"""
智能理财助手 - 主程序入口
"""
import os
import sys
import json
from datetime import date
from typing import List, Dict, Optional
from loguru import logger

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from src.models import FundType
from src.portfolio import PortfolioManager
from src.collector import fund_collector, valuation_collector, news_collector
from src.analyzer import ai_advisor
from src.decision import DecisionEngine
from src.report import report_generator
from src.notify import notifier, wecom_bot

# 量化选基模块
from src.workflow.fund_analysis import FundAnalysisWorkflow, fund_analysis_workflow
from src.storage.fund_storage import fund_storage


class InvestmentAdvisor:
    """智能理财助手"""
    
    def __init__(self):
        # 初始化各模块
        self.portfolio_manager = PortfolioManager()
        self.decision_engine = DecisionEngine(self.portfolio_manager.portfolio)
        
        # 关注列表（可自定义）
        self.watch_list: List[Dict] = []
        
        # 加载关注列表
        self._load_watch_list()
        
        logger.info("智能理财助手初始化完成")
    
    def _load_watch_list(self):
        """加载关注列表"""
        watch_list_file = os.path.join(settings.DATA_DIR, "watch_list.json")
        if os.path.exists(watch_list_file):
            with open(watch_list_file, 'r', encoding='utf-8') as f:
                self.watch_list = json.load(f)
    
    def _save_watch_list(self):
        """保存关注列表"""
        os.makedirs(settings.DATA_DIR, exist_ok=True)
        watch_list_file = os.path.join(settings.DATA_DIR, "watch_list.json")
        with open(watch_list_file, 'w', encoding='utf-8') as f:
            json.dump(self.watch_list, f, ensure_ascii=False, indent=2)
    
    def initialize(self, initial_cash: float):
        """初始化投资组合
        
        Args:
            initial_cash: 初始资金
        """
        self.portfolio_manager.initialize_portfolio(initial_cash)
        logger.info(f"投资组合已初始化，初始资金: ¥{initial_cash:,.2f}")
    
    def add_to_watch_list(self, fund_code: str, fund_name: str = ""):
        """添加基金到关注列表"""
        # 获取基金名称
        if not fund_name:
            nav = fund_collector.get_fund_estimate(fund_code)
            if nav:
                fund_name = nav.name
        
        # 检查是否已存在
        for item in self.watch_list:
            if item["code"] == fund_code:
                logger.info(f"基金 {fund_code} 已在关注列表中")
                return
        
        self.watch_list.append({
            "code": fund_code,
            "name": fund_name or fund_code
        })
        self._save_watch_list()
        logger.info(f"已添加 {fund_name or fund_code} 到关注列表")
    
    def remove_from_watch_list(self, fund_code: str):
        """从关注列表移除基金"""
        self.watch_list = [
            item for item in self.watch_list 
            if item["code"] != fund_code
        ]
        self._save_watch_list()
        logger.info(f"已从关注列表移除 {fund_code}")
    
    def run_daily_analysis(self) -> str:
        """运行每日分析并生成报告
        
        Returns:
            生成的报告文本
        """
        logger.info("="*60)
        logger.info("开始每日分析...")
        logger.info("="*60)
        
        # 1. 更新持仓净值
        self._update_portfolio_prices()
        
        # 2. 运行分析
        analysis_result = self.decision_engine.run_daily_analysis(self.watch_list)
        
        # 3. 生成报告
        portfolio_summary = self.portfolio_manager.get_portfolio_summary()
        position_details = self.portfolio_manager.get_position_details()
        
        report_text = report_generator.generate_daily_report(
            analysis_result,
            portfolio_summary,
            position_details
        )
        
        logger.info("每日分析完成")
        return report_text
    
    def run_and_notify(self, use_wecom: bool = True):
        """运行分析并发送通知
        
        Args:
            use_wecom: 是否使用企业微信发送（默认True）
        """
        # 运行分析
        logger.info("="*60)
        logger.info("开始每日分析...")
        logger.info("="*60)
        
        # 更新持仓净值
        self._update_portfolio_prices()
        
        # 运行分析
        analysis_result = self.decision_engine.run_daily_analysis(self.watch_list)
        
        # 获取摘要数据
        portfolio_summary = self.portfolio_manager.get_portfolio_summary()
        position_details = self.portfolio_manager.get_position_details()
        
        # 生成文本报告
        report_text = report_generator.generate_daily_report(
            analysis_result,
            portfolio_summary,
            position_details
        )
        
        # 发送通知
        results = {}
        
        # 企业微信 - 使用格式化消息
        if use_wecom and wecom_bot.enabled:
            market_analysis = analysis_result.get("market_analysis", {})
            suggestions = analysis_result.get("suggestions", {})
            
            wecom_result = wecom_bot.send_daily_report(
                portfolio_summary,
                market_analysis,
                suggestions
            )
            results["wecom"] = wecom_result
            
            # 如果有卖出建议（止盈止损），单独发送提醒
            for sell_suggestion in suggestions.get("sell", []):
                wecom_bot.send_trade_alert(
                    trade_type="sell",
                    fund_name=sell_suggestion.get("fund_name", ""),
                    fund_code=sell_suggestion.get("fund_code", ""),
                    reason=", ".join(sell_suggestion.get("reasons", [])),
                    amount=sell_suggestion.get("suggested_amount"),
                    confidence=sell_suggestion.get("confidence", 3)
                )
        
        # 其他通知渠道
        other_results = notifier.send_daily_report(report_text)
        results.update(other_results)
        
        logger.info(f"通知发送结果: {results}")
        return report_text
    
    def _update_portfolio_prices(self):
        """更新持仓净值"""
        if not self.portfolio_manager.portfolio.positions:
            return
        
        fund_codes = [p.fund_code for p in self.portfolio_manager.portfolio.positions]
        nav_dict = {}
        
        for code in fund_codes:
            nav = fund_collector.get_fund_estimate(code)
            if nav:
                nav_dict[code] = nav.nav
        
        if nav_dict:
            self.portfolio_manager.update_prices(nav_dict)
    
    def buy_fund(
        self,
        fund_code: str,
        amount: float,
        reason: str = ""
    ) -> bool:
        """买入基金
        
        Args:
            fund_code: 基金代码
            amount: 买入金额
            reason: 买入理由
        
        Returns:
            是否成功
        """
        # 获取基金信息
        nav = fund_collector.get_fund_estimate(fund_code)
        if not nav:
            logger.error(f"无法获取基金 {fund_code} 的净值")
            return False
        
        # 计算份额
        shares = amount / nav.nav
        
        # 检测基金类型
        fund_type = fund_collector.detect_fund_type(fund_code, nav.name)
        
        # 风控检查
        check_result = self.portfolio_manager.check_position_limits(
            fund_code, fund_type, amount
        )
        
        if not check_result["allowed"]:
            logger.warning(f"风控检查未通过: {check_result['warnings']}")
            return False
        
        if check_result["warnings"]:
            for warning in check_result["warnings"]:
                logger.warning(f"风险提示: {warning}")
        
        # 执行买入
        return self.portfolio_manager.add_position(
            fund_code=fund_code,
            fund_name=nav.name,
            fund_type=fund_type,
            shares=shares,
            price=nav.nav,
            reason=reason
        )
    
    def sell_fund(
        self,
        fund_code: str,
        shares: float = None,
        ratio: float = None,
        reason: str = ""
    ) -> bool:
        """卖出基金
        
        Args:
            fund_code: 基金代码
            shares: 卖出份额（与ratio二选一）
            ratio: 卖出比例（0-1）
            reason: 卖出理由
        
        Returns:
            是否成功
        """
        position = self.portfolio_manager.portfolio.get_position(fund_code)
        if not position:
            logger.error(f"未持有基金 {fund_code}")
            return False
        
        # 确定卖出份额
        if ratio is not None:
            shares = position.shares * ratio
        elif shares is None:
            shares = position.shares  # 默认全部卖出
        
        # 获取最新净值
        nav = fund_collector.get_fund_estimate(fund_code)
        price = nav.nav if nav else position.current_price
        
        return self.portfolio_manager.reduce_position(
            fund_code=fund_code,
            shares=shares,
            price=price,
            reason=reason
        )
    
    def get_fund_suggestion(self, fund_code: str) -> Optional[Dict]:
        """获取单只基金的建议
        
        Args:
            fund_code: 基金代码
        
        Returns:
            交易建议
        """
        suggestion = self.decision_engine.get_quick_suggestion(fund_code)
        if suggestion:
            return suggestion.model_dump()
        return None
    
    def show_portfolio(self):
        """显示当前持仓"""
        summary = self.portfolio_manager.get_portfolio_summary()
        details = self.portfolio_manager.get_position_details()
        
        print("\n" + "="*60)
        print("📊 当前持仓")
        print("="*60)
        print(f"总资产: ¥{summary['total_value']:,.2f}")
        print(f"现金: ¥{summary['cash']:,.2f}")
        print(f"仓位: {summary['position_ratio']*100:.1f}%")
        print(f"总收益: ¥{summary['total_profit']:,.2f} ({summary['total_profit_rate']*100:.2f}%)")
        print("-"*60)
        
        if details:
            for pos in details:
                profit_sign = "+" if pos['profit_rate'] >= 0 else ""
                print(f"\n{pos['fund_name']} ({pos['fund_code']})")
                print(f"  类型: {pos['fund_type']}")
                print(f"  份额: {pos['shares']:,.2f}")
                print(f"  市值: ¥{pos['market_value']:,.2f}")
                print(f"  收益: {profit_sign}{pos['profit_rate']*100:.2f}%")
        else:
            print("\n暂无持仓")
        
        print("="*60 + "\n")
    
    def show_market_overview(self):
        """显示市场概览"""
        print("\n" + "="*60)
        print("📈 市场概览")
        print("="*60)
        
        # 获取市场数据
        market = news_collector.get_market_summary()
        print(f"上证指数: {market.sh_index:.2f} ({market.sh_change*100:+.2f}%)")
        print(f"深证成指: {market.sz_index:.2f} ({market.sz_change*100:+.2f}%)")
        print(f"沪深300: {market.hs300_index:.2f} ({market.hs300_change*100:+.2f}%)")
        print(f"市场情绪: {market.market_sentiment}")
        
        print("-"*60)
        
        # 获取估值数据
        valuation = valuation_collector.get_market_overall_valuation()
        print(f"估值水平: {valuation['level']}")
        print(f"PE百分位: {valuation['pe_percentile']:.1f}%")
        print(f"建议: {valuation['suggestion']}")
        
        print("="*60 + "\n")
    
    # ============ 量化选基功能 ============
    
    def run_fund_screening(
        self, 
        fund_types: list = None,
        top_n: int = 20
    ) -> dict:
        """运行量化选基分析
        
        Args:
            fund_types: 要分析的基金类型列表
            top_n: 每类保留前 N 名
            
        Returns:
            分析结果
        """
        logger.info("开始量化选基分析...")
        
        workflow = FundAnalysisWorkflow()
        result = workflow.run_full_analysis(
            fund_types=fund_types,
            top_n=top_n,
            use_cache=True,
            save_results=True
        )
        
        return result
    
    def analyze_fund(self, fund_code: str) -> dict:
        """分析单只基金
        
        Args:
            fund_code: 基金代码
            
        Returns:
            分析结果
        """
        workflow = FundAnalysisWorkflow()
        return workflow.analyze_single_fund(fund_code)
    
    def show_top_funds(
        self, 
        fund_type: str = 'all',
        top_n: int = 10
    ):
        """显示推荐基金列表
        
        Args:
            fund_type: 基金类型
            top_n: 显示数量
        """
        print("\n" + "="*60)
        print(f"📊 {fund_type} 基金推荐 TOP {top_n}")
        print("="*60)
        
        workflow = FundAnalysisWorkflow()
        recommendations = workflow.get_top_recommendations(fund_type, top_n)
        
        if not recommendations:
            print("暂无推荐数据，请先运行 screen 命令进行分析")
            return
        
        for i, rec in enumerate(recommendations, 1):
            grade = rec.get('grade', '-')
            score = rec.get('total_score', 0)
            name = rec.get('fund_name', '')[:12]  # 截断过长的名字
            code = rec.get('fund_code', '')
            
            # 根据评级显示不同颜色
            grade_icon = {'A': '🌟', 'B': '⭐', 'C': '✨', 'D': '💫', 'E': '✦'}.get(grade, '·')
            
            print(f"  {i:2d}. {grade_icon} [{grade}] {score:.1f}分  {name}({code})")
        
        print("="*60)
        print("评分等级: A(≥80) B(≥70) C(≥60) D(≥50) E(<50)")
        print("="*60 + "\n")
    
    def show_fund_analysis(self, fund_code: str):
        """显示单只基金的详细分析
        
        Args:
            fund_code: 基金代码
        """
        print(f"\n正在分析基金 {fund_code}...")
        
        result = self.analyze_fund(fund_code)
        
        if not result.get('success'):
            print(f"❌ 分析失败: {result.get('error', '未知错误')}")
            return
        
        print("\n" + "="*60)
        print(f"📊 基金分析报告: {fund_code}")
        print("="*60)
        
        # 评分信息
        score = result.get('score', {})
        total_score = score.get('total_score', 0)
        
        rec = result.get('recommendation', {})
        grade = rec.get('grade', '-')
        action = rec.get('action', '-')
        
        print(f"\n综合评分: {total_score:.1f} / 100  等级: {grade}")
        print(f"投资建议: {action}")
        
        # 分类得分
        cat_scores = score.get('category_scores', {})
        if cat_scores:
            print("\n分类得分:")
            score_names = {
                'return': '收益能力',
                'risk': '风险控制',
                'risk_adjusted': '风险调整收益',
                'scale': '规模因子',
                'manager': '基金经理',
                'style': '风格稳定性'
            }
            for cat, cat_score in cat_scores.items():
                name = score_names.get(cat, cat)
                bar = '█' * int(cat_score / 10) + '░' * (10 - int(cat_score / 10))
                print(f"  {name}: {bar} {cat_score:.1f}")
        
        # 预筛选结果
        prefilter = result.get('prefilter_passed', False)
        print(f"\n4433筛选: {'✅ 通过' if prefilter else '❌ 未通过'}")
        
        # 投资建议
        reasons = rec.get('reasons', [])
        risks = rec.get('risks', [])
        
        if reasons:
            print("\n✅ 优势:")
            for r in reasons:
                print(f"  · {r}")
        
        if risks:
            print("\n⚠️ 风险提示:")
            for r in risks:
                print(f"  · {r}")
        
        print("\n" + "="*60 + "\n")


def main():
    """主函数 - 交互式命令行界面"""
    advisor = InvestmentAdvisor()
    
    print("""
╔══════════════════════════════════════════════════════════════╗
║                    智能理财助手 v1.1                          ║
║                                                              ║
║  基础命令:                                                   ║
║    init <金额>      - 初始化投资组合                          ║
║    buy <代码> <金额> - 买入基金                               ║
║    sell <代码> [比例] - 卖出基金                              ║
║    watch <代码>     - 添加到关注列表                          ║
║    unwatch <代码>   - 从关注列表移除                          ║
║    portfolio       - 查看持仓                                 ║
║                                                              ║
║  分析命令:                                                   ║
║    analyze         - 运行每日分析                             ║
║    suggest <代码>  - 获取基金建议                             ║
║    market          - 查看市场概览                             ║
║                                                              ║
║  量化选基（新功能）:                                          ║
║    screen [类型]   - 运行量化选基分析                          ║
║    top [类型] [N]  - 查看推荐基金 TOP N                        ║
║    detail <代码>   - 查看基金详细分析                          ║
║                                                              ║
║  通知命令:                                                   ║
║    notify          - 运行分析并发送企业微信通知                ║
║    test_wecom      - 测试企业微信连接                         ║
║                                                              ║
║    help            - 显示帮助                                 ║
║    quit            - 退出                                     ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    while True:
        try:
            cmd = input("\n> ").strip().split()
            if not cmd:
                continue
            
            action = cmd[0].lower()
            
            if action == "quit" or action == "exit":
                print("再见！祝投资顺利！")
                break
            
            elif action == "init":
                if len(cmd) < 2:
                    print("用法: init <金额>")
                    continue
                amount = float(cmd[1])
                advisor.initialize(amount)
            
            elif action == "buy":
                if len(cmd) < 3:
                    print("用法: buy <基金代码> <金额>")
                    continue
                fund_code = cmd[1]
                amount = float(cmd[2])
                reason = " ".join(cmd[3:]) if len(cmd) > 3 else ""
                success = advisor.buy_fund(fund_code, amount, reason)
                if success:
                    print(f"✅ 买入成功")
                else:
                    print(f"❌ 买入失败")
            
            elif action == "sell":
                if len(cmd) < 2:
                    print("用法: sell <基金代码> [卖出比例0-1]")
                    continue
                fund_code = cmd[1]
                ratio = float(cmd[2]) if len(cmd) > 2 else 1.0
                reason = " ".join(cmd[3:]) if len(cmd) > 3 else ""
                success = advisor.sell_fund(fund_code, ratio=ratio, reason=reason)
                if success:
                    print(f"✅ 卖出成功")
                else:
                    print(f"❌ 卖出失败")
            
            elif action == "watch":
                if len(cmd) < 2:
                    print("用法: watch <基金代码>")
                    continue
                advisor.add_to_watch_list(cmd[1])
            
            elif action == "unwatch":
                if len(cmd) < 2:
                    print("用法: unwatch <基金代码>")
                    continue
                advisor.remove_from_watch_list(cmd[1])
            
            elif action == "analyze":
                print("正在分析，请稍候...")
                report = advisor.run_daily_analysis()
                print(report)
            
            elif action == "notify":
                print("正在分析并发送企业微信通知...")
                if not wecom_bot.enabled:
                    print("❌ 企业微信未配置，请先配置 WECOM_WEBHOOK")
                    print("   参考文档: docs/企业微信配置指南.md")
                    continue
                report = advisor.run_and_notify(use_wecom=True)
                print("✅ 分析完成，通知已发送")
                print(report)
            
            elif action == "test_wecom":
                print("正在测试企业微信连接...")
                if not wecom_bot.enabled:
                    print("❌ 企业微信未配置")
                    print("   请在 .env 文件中设置 WECOM_WEBHOOK")
                    print("   参考文档: docs/企业微信配置指南.md")
                else:
                    success = wecom_bot.send_test_message()
                    if success:
                        print("✅ 企业微信测试消息发送成功！请检查您的企业微信群")
                    else:
                        print("❌ 企业微信测试消息发送失败，请检查Webhook配置")
            
            elif action == "suggest":
                if len(cmd) < 2:
                    print("用法: suggest <基金代码>")
                    continue
                suggestion = advisor.get_fund_suggestion(cmd[1])
                if suggestion:
                    print(f"\n基金: {suggestion['fund_name']} ({suggestion['fund_code']})")
                    print(f"建议: {suggestion['signal']}")
                    print(f"置信度: {'★' * suggestion['confidence'] + '☆' * (5 - suggestion['confidence'])}")
                    print(f"理由: {', '.join(suggestion['reasons'])}")
                else:
                    print("无法获取建议")
            
            elif action == "portfolio":
                advisor.show_portfolio()
            
            elif action == "market":
                advisor.show_market_overview()
            
            # ===== 量化选基命令 =====
            elif action == "screen":
                # 运行量化选基分析
                fund_types = None
                if len(cmd) > 1:
                    # 支持指定类型，如 screen 股票型 混合型
                    fund_types = cmd[1:]
                
                print("正在运行量化选基分析，这可能需要几分钟时间...")
                result = advisor.run_fund_screening(fund_types=fund_types)
                
                print(f"\n✅ 分析完成!")
                print(f"耗时: {result.get('elapsed_seconds', 0):.1f} 秒")
                
                stats = result.get('statistics', {})
                for fund_type, type_stats in stats.items():
                    print(f"\n{fund_type}:")
                    print(f"  分析: {type_stats.get('analyzed', 0)} 只")
                    print(f"  通过筛选: {type_stats.get('passed_prefilter', 0)} 只")
                
                print("\n使用 'top [类型]' 命令查看推荐基金列表")
            
            elif action == "top":
                # 查看推荐基金
                fund_type = cmd[1] if len(cmd) > 1 else 'all'
                top_n = int(cmd[2]) if len(cmd) > 2 else 10
                advisor.show_top_funds(fund_type, top_n)
            
            elif action == "detail":
                # 查看基金详细分析
                if len(cmd) < 2:
                    print("用法: detail <基金代码>")
                    continue
                advisor.show_fund_analysis(cmd[1])
            
            elif action == "help":
                print("""
命令列表:

【基础命令】
  init <金额>       - 初始化投资组合
  buy <代码> <金额> - 买入基金
  sell <代码> [比例] - 卖出基金（比例0-1，默认全部）
  watch <代码>      - 添加到关注列表
  unwatch <代码>    - 从关注列表移除
  portfolio        - 查看当前持仓

【分析命令】
  analyze          - 运行每日分析生成报告
  suggest <代码>   - 获取单只基金的建议
  market           - 查看市场概览

【量化选基】
  screen [类型...]  - 运行量化选基分析
                     类型可选: 股票型 混合型 指数型 债券型
                     例: screen 股票型 混合型
  top [类型] [N]   - 查看推荐基金 TOP N（默认10）
                     例: top 股票型 20
  detail <代码>    - 查看基金详细分析
                     例: detail 000001

【通知命令】
  notify           - 运行分析并发送企业微信通知
  test_wecom       - 测试企业微信机器人连接

【其他】
  help             - 显示帮助
  quit             - 退出程序
                """)
            
            else:
                print(f"未知命令: {action}，输入 help 查看帮助")
        
        except KeyboardInterrupt:
            print("\n再见！")
            break
        except Exception as e:
            logger.error(f"执行命令时出错: {e}")
            print(f"❌ 错误: {e}")


if __name__ == "__main__":
    main()
