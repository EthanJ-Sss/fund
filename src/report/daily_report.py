"""
每日报告生成模块
"""
import os
import json
from datetime import date, datetime
from typing import Dict, List, Optional
from loguru import logger

from config.settings import settings
from ..models import DailyReport, TradeSuggestion, SignalType


class ReportGenerator:
    """报告生成器"""
    
    def __init__(self):
        self.reports_dir = settings.DAILY_REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def generate_daily_report(
        self,
        analysis_result: Dict,
        portfolio_summary: Dict,
        position_details: List[Dict]
    ) -> str:
        """生成每日报告
        
        Args:
            analysis_result: 决策引擎的分析结果
            portfolio_summary: 持仓概览
            position_details: 持仓明细
        
        Returns:
            格式化的报告文本
        """
        report_date = date.today()
        
        # 构建报告
        report_lines = []
        
        # 报告头部
        report_lines.append(self._generate_header(report_date))
        
        # 持仓概览
        report_lines.append(self._generate_portfolio_summary(portfolio_summary))
        
        # 持仓明细
        if position_details:
            report_lines.append(self._generate_position_details(position_details))
        
        # 市场概况
        if analysis_result.get("market_analysis"):
            report_lines.append(self._generate_market_summary(analysis_result["market_analysis"]))
        
        # 交易建议
        report_lines.append(self._generate_suggestions(analysis_result.get("suggestions", {})))
        
        # 风险提示
        if analysis_result.get("risk_warnings"):
            report_lines.append(self._generate_risk_warnings(analysis_result["risk_warnings"]))
        
        # 执行摘要
        if analysis_result.get("execution_summary"):
            report_lines.append(self._generate_execution_summary(analysis_result["execution_summary"]))
        
        # 报告尾部
        report_lines.append(self._generate_footer())
        
        report_text = "\n".join(report_lines)
        
        # 保存报告
        self._save_report(report_date, report_text, analysis_result)
        
        return report_text
    
    def _generate_header(self, report_date: date) -> str:
        """生成报告头部"""
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday = weekday_names[report_date.weekday()]
        
        return f"""
╔══════════════════════════════════════════════════════════════════╗
║                     【每日理财报告】                              ║
║                   {report_date.strftime('%Y年%m月%d日')} {weekday}                         ║
╚══════════════════════════════════════════════════════════════════╝
"""
    
    def _generate_portfolio_summary(self, summary: Dict) -> str:
        """生成持仓概览"""
        total_value = summary.get("total_value", 0)
        cash = summary.get("cash", 0)
        position_value = summary.get("position_value", 0)
        total_profit = summary.get("total_profit", 0)
        total_profit_rate = summary.get("total_profit_rate", 0)
        position_ratio = summary.get("position_ratio", 0)
        
        profit_sign = "+" if total_profit >= 0 else ""
        
        return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 持仓概览
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  总资产：¥{total_value:>12,.2f}
  ├── 持仓市值：¥{position_value:>10,.2f} ({position_ratio*100:.1f}%)
  └── 现金余额：¥{cash:>10,.2f} ({(1-position_ratio)*100:.1f}%)
  
  总收益：{profit_sign}¥{total_profit:,.2f} ({profit_sign}{total_profit_rate*100:.2f}%)
"""
    
    def _generate_position_details(self, positions: List[Dict]) -> str:
        """生成持仓明细"""
        lines = ["""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 持仓明细
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""]
        
        for pos in positions:
            profit_rate = pos.get("profit_rate", 0)
            profit_sign = "+" if profit_rate >= 0 else ""
            profit_color = "📈" if profit_rate >= 0 else "📉"
            
            lines.append(f"""
  ┌────────────────────────────────────────────────────────────┐
  │ {pos.get('fund_name', '未知')}({pos.get('fund_code', '')})
  │ 类型: {pos.get('fund_type', '未知')}
  ├────────────────────────────────────────────────────────────┤
  │ 持有份额: {pos.get('shares', 0):,.2f}份
  │ 成本价: {pos.get('cost_price', 0):.4f}  |  当前净值: {pos.get('current_price', 0):.4f}
  │ 市值: ¥{pos.get('market_value', 0):,.2f}  |  仓位占比: {pos.get('position_ratio', 0)*100:.1f}%
  │ {profit_color} 收益: {profit_sign}{pos.get('profit_loss', 0):,.2f} ({profit_sign}{profit_rate*100:.2f}%)
  │ 持有天数: {pos.get('hold_days', 0)}天
  └────────────────────────────────────────────────────────────┘
""")
        
        return "\n".join(lines)
    
    def _generate_market_summary(self, market_analysis: Dict) -> str:
        """生成市场概况"""
        return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📰 市场概况
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  市场趋势：{market_analysis.get('market_trend', '未知')}
  估值水平：{market_analysis.get('valuation_level', '未知')}
  风险等级：{market_analysis.get('risk_level', '未知')}
  建议最大仓位：{market_analysis.get('max_position_ratio', 0.6)*100:.0f}%
  
  关键观察：
  {self._format_list(market_analysis.get('key_observations', []))}
  
  操作建议：{market_analysis.get('operation_suggestion', '无')}
"""
    
    def _generate_suggestions(self, suggestions: Dict) -> str:
        """生成交易建议"""
        lines = ["""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 交易建议
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""]
        
        # 卖出建议
        sell_list = suggestions.get("sell", [])
        if sell_list:
            lines.append("\n  【卖出建议】⚠️")
            for s in sell_list:
                lines.append(self._format_suggestion(s, "卖出"))
        else:
            lines.append("\n  【卖出建议】无")
        
        # 买入建议
        buy_list = suggestions.get("buy", [])
        if buy_list:
            lines.append("\n  【买入建议】✅")
            for s in buy_list:
                lines.append(self._format_suggestion(s, "买入"))
        else:
            lines.append("\n  【买入建议】无")
        
        # 持有建议
        hold_list = suggestions.get("hold", [])
        if hold_list:
            lines.append("\n  【持有建议】📌")
            for s in hold_list:
                lines.append(self._format_suggestion(s, "持有"))
        
        # 观望列表
        watch_list = suggestions.get("watch", [])
        if watch_list:
            lines.append("\n  【观望列表】👀")
            for s in watch_list:
                lines.append(f"    - {s.get('fund_name', '')}({s.get('fund_code', '')})")
        
        return "\n".join(lines)
    
    def _format_suggestion(self, suggestion: Dict, action: str) -> str:
        """格式化单条建议"""
        fund_name = suggestion.get("fund_name", "未知")
        fund_code = suggestion.get("fund_code", "")
        confidence = suggestion.get("confidence", 3)
        stars = "★" * confidence + "☆" * (5 - confidence)
        reasons = suggestion.get("reasons", [])
        amount = suggestion.get("suggested_amount")
        warnings = suggestion.get("risk_warnings", [])
        
        lines = [f"""
    ┌─ {fund_name}({fund_code})
    │  置信度: {stars}"""]
        
        if amount:
            if action == "买入":
                lines.append(f"    │  建议金额: ¥{amount:,.2f}")
            elif action == "卖出":
                lines.append(f"    │  建议份额: {amount:,.2f}份")
        
        if reasons:
            lines.append(f"    │  理由:")
            for r in reasons[:3]:  # 最多显示3条
                lines.append(f"    │    • {r}")
        
        if warnings:
            lines.append(f"    │  ⚠️ 风险提示:")
            for w in warnings[:2]:  # 最多显示2条
                lines.append(f"    │    • {w}")
        
        lines.append("    └─────────────────────────────────────────")
        
        return "\n".join(lines)
    
    def _generate_risk_warnings(self, warnings: List[str]) -> str:
        """生成风险提示"""
        return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 风险提示
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{self._format_list(warnings, prefix='  ⚠️ ')}
"""
    
    def _generate_execution_summary(self, summary: List[str]) -> str:
        """生成执行摘要"""
        return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 执行摘要
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{self._format_list(summary, prefix='  • ')}
"""
    
    def _generate_footer(self) -> str:
        """生成报告尾部"""
        return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📌 免责声明
  本报告由AI系统自动生成，仅供参考，不构成投资建议。
  投资有风险，入市需谨慎。请根据个人情况谨慎决策。

  报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    def _format_list(self, items: List[str], prefix: str = "  • ") -> str:
        """格式化列表"""
        if not items:
            return f"{prefix}无"
        return "\n".join([f"{prefix}{item}" for item in items])
    
    def _save_report(self, report_date: date, report_text: str, analysis_data: Dict):
        """保存报告"""
        # 保存文本报告
        text_file = os.path.join(
            self.reports_dir, 
            f"report_{report_date.strftime('%Y%m%d')}.txt"
        )
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        # 保存JSON数据
        json_file = os.path.join(
            self.reports_dir,
            f"report_{report_date.strftime('%Y%m%d')}.json"
        )
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"报告已保存: {text_file}")
    
    def get_report_history(self, days: int = 7) -> List[str]:
        """获取历史报告列表"""
        reports = []
        for filename in sorted(os.listdir(self.reports_dir), reverse=True):
            if filename.startswith("report_") and filename.endswith(".txt"):
                reports.append(filename)
                if len(reports) >= days:
                    break
        return reports
    
    def load_report(self, report_date: date) -> Optional[str]:
        """加载指定日期的报告"""
        text_file = os.path.join(
            self.reports_dir,
            f"report_{report_date.strftime('%Y%m%d')}.txt"
        )
        if os.path.exists(text_file):
            with open(text_file, 'r', encoding='utf-8') as f:
                return f.read()
        return None


# 创建全局实例
report_generator = ReportGenerator()
