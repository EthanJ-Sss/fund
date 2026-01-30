"""
企业微信机器人模块
提供丰富的消息格式和便捷的发送功能
"""
import json
import requests
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from loguru import logger

from config.settings import settings


class WeComBot:
    """企业微信机器人"""
    
    def __init__(self, webhook_url: str = None):
        self.webhook_url = webhook_url or settings.Notification.WECOM_WEBHOOK
        self.enabled = bool(self.webhook_url)
        
        if not self.enabled:
            logger.warning("企业微信Webhook未配置，消息将不会发送")
    
    def _send_request(self, data: dict) -> bool:
        """发送请求到企业微信"""
        if not self.enabled:
            logger.warning("企业微信未配置，跳过发送")
            return False
        
        try:
            response = requests.post(
                self.webhook_url,
                json=data,
                timeout=10
            )
            result = response.json()
            
            if result.get("errcode") == 0:
                logger.info("企业微信消息发送成功")
                return True
            else:
                logger.error(f"企业微信消息发送失败: {result}")
                return False
                
        except Exception as e:
            logger.error(f"企业微信请求异常: {e}")
            return False
    
    def send_text(
        self, 
        content: str, 
        mentioned_list: List[str] = None,
        mentioned_mobile_list: List[str] = None
    ) -> bool:
        """发送文本消息
        
        Args:
            content: 消息内容，最长不超过2048个字节
            mentioned_list: @的成员userid列表，@all表示所有人
            mentioned_mobile_list: @的成员手机号列表
        """
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        
        if mentioned_list:
            data["text"]["mentioned_list"] = mentioned_list
        if mentioned_mobile_list:
            data["text"]["mentioned_mobile_list"] = mentioned_mobile_list
        
        return self._send_request(data)
    
    def send_markdown(self, content: str) -> bool:
        """发送Markdown消息
        
        支持的语法：
        - 标题：# ## ### ####
        - 加粗：**text**
        - 链接：[text](url)
        - 行内代码：`code`
        - 引用：> quote
        - 颜色：<font color="info/comment/warning">text</font>
        
        Args:
            content: Markdown格式的消息内容
        """
        # 确保内容不超过4096字节
        if len(content.encode('utf-8')) > 4096:
            content = self._truncate_content(content, 4000)
            content += "\n\n*...内容已截断，请查看完整报告*"
        
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        
        return self._send_request(data)
    
    def send_news(
        self,
        articles: List[Dict[str, str]]
    ) -> bool:
        """发送图文消息（卡片形式）
        
        Args:
            articles: 图文列表，每个元素包含：
                - title: 标题
                - description: 描述（可选）
                - url: 点击跳转链接
                - picurl: 图片链接（可选）
        """
        data = {
            "msgtype": "news",
            "news": {
                "articles": articles[:8]  # 最多8条
            }
        }
        
        return self._send_request(data)
    
    def send_template_card(
        self,
        card_type: str,
        main_title: str,
        sub_title: str = "",
        horizontal_content_list: List[Dict] = None,
        jump_list: List[Dict] = None
    ) -> bool:
        """发送模板卡片消息
        
        Args:
            card_type: 卡片类型，text_notice
            main_title: 主标题
            sub_title: 副标题
            horizontal_content_list: 二级标题+文本列表
            jump_list: 跳转链接列表
        """
        data = {
            "msgtype": "template_card",
            "template_card": {
                "card_type": card_type,
                "main_title": {
                    "title": main_title,
                    "desc": sub_title
                }
            }
        }
        
        if horizontal_content_list:
            data["template_card"]["horizontal_content_list"] = horizontal_content_list
        
        if jump_list:
            data["template_card"]["jump_list"] = jump_list
        
        return self._send_request(data)
    
    # ==================== 业务消息模板 ====================
    
    def send_daily_report(
        self,
        portfolio_summary: Dict,
        market_analysis: Dict,
        suggestions: Dict
    ) -> bool:
        """发送每日报告
        
        Args:
            portfolio_summary: 持仓概览
            market_analysis: 市场分析
            suggestions: 交易建议
        """
        total_value = portfolio_summary.get("total_value", 0)
        total_profit = portfolio_summary.get("total_profit", 0)
        total_profit_rate = portfolio_summary.get("total_profit_rate", 0)
        position_ratio = portfolio_summary.get("position_ratio", 0)
        
        profit_color = "info" if total_profit >= 0 else "warning"
        profit_sign = "+" if total_profit >= 0 else ""
        
        # 构建Markdown消息
        content = f"""# 📊 每日理财报告
> {date.today().strftime('%Y年%m月%d日')}

## 💰 持仓概览
- 总资产: **¥{total_value:,.2f}**
- 总收益: <font color="{profit_color}">{profit_sign}¥{total_profit:,.2f} ({profit_sign}{total_profit_rate*100:.2f}%)</font>
- 当前仓位: **{position_ratio*100:.1f}%**

## 📈 市场状况
- 市场趋势: **{market_analysis.get('market_trend', '未知')}**
- 估值水平: **{market_analysis.get('valuation_level', '未知')}**
- 风险等级: **{market_analysis.get('risk_level', '未知')}**
"""
        
        # 添加交易建议
        buy_list = suggestions.get("buy", [])
        sell_list = suggestions.get("sell", [])
        
        if sell_list:
            content += "\n## ⚠️ 卖出建议\n"
            for s in sell_list[:3]:
                stars = "★" * s.get("confidence", 3) + "☆" * (5 - s.get("confidence", 3))
                content += f"- **{s.get('fund_name', '')}** {stars}\n"
        
        if buy_list:
            content += "\n## ✅ 买入建议\n"
            for s in buy_list[:3]:
                stars = "★" * s.get("confidence", 3) + "☆" * (5 - s.get("confidence", 3))
                amount = s.get("suggested_amount", 0)
                content += f"- **{s.get('fund_name', '')}** ¥{amount:,.0f} {stars}\n"
        
        if not buy_list and not sell_list:
            content += "\n## 📌 今日建议\n- 无需操作，保持观望\n"
        
        content += f"\n---\n*生成时间: {datetime.now().strftime('%H:%M:%S')}*"
        
        return self.send_markdown(content)
    
    def send_market_alert(
        self,
        alert_type: str,
        index_name: str,
        change_pct: float,
        message: str = ""
    ) -> bool:
        """发送市场预警
        
        Args:
            alert_type: 预警类型 (crash/surge)
            index_name: 指数名称
            change_pct: 涨跌幅
            message: 附加消息
        """
        if alert_type == "crash":
            emoji = "🔴"
            title = "市场大跌预警"
            color = "warning"
        else:
            emoji = "🟢"
            title = "市场大涨提醒"
            color = "info"
        
        content = f"""# {emoji} {title}

**{index_name}** 今日涨跌幅: <font color="{color}">{change_pct*100:+.2f}%</font>

{message}

> ⚠️ 系统已自动暂停交易操作，请谨慎决策

*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""
        
        return self.send_markdown(content)
    
    def send_trade_alert(
        self,
        trade_type: str,
        fund_name: str,
        fund_code: str,
        reason: str,
        amount: float = None,
        confidence: int = 3
    ) -> bool:
        """发送交易提醒
        
        Args:
            trade_type: 交易类型 (buy/sell/stop_loss/take_profit)
            fund_name: 基金名称
            fund_code: 基金代码
            reason: 原因
            amount: 建议金额/份额
            confidence: 置信度 1-5
        """
        type_config = {
            "buy": ("💰", "买入建议", "info"),
            "sell": ("📤", "卖出建议", "comment"),
            "stop_loss": ("🛑", "止损提醒", "warning"),
            "take_profit": ("🎯", "止盈提醒", "info"),
        }
        
        emoji, title, color = type_config.get(trade_type, ("📌", "交易提醒", "comment"))
        stars = "★" * confidence + "☆" * (5 - confidence)
        
        content = f"""# {emoji} {title}

**{fund_name}** (`{fund_code}`)

- 置信度: {stars}
- 原因: {reason}
"""
        
        if amount:
            if trade_type in ["buy"]:
                content += f'- 建议金额: <font color="{color}">¥{amount:,.2f}</font>\n'
            else:
                content += f'- 建议份额: <font color="{color}">{amount:,.2f}份</font>\n'
        
        content += f"\n*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        
        return self.send_markdown(content)
    
    def send_position_update(
        self,
        fund_name: str,
        fund_code: str,
        current_price: float,
        profit_rate: float,
        action: str = None
    ) -> bool:
        """发送持仓更新
        
        Args:
            fund_name: 基金名称
            fund_code: 基金代码
            current_price: 当前净值
            profit_rate: 收益率
            action: 操作建议
        """
        profit_color = "info" if profit_rate >= 0 else "warning"
        profit_sign = "+" if profit_rate >= 0 else ""
        
        content = f"""## 📊 持仓更新

**{fund_name}** (`{fund_code}`)

- 当前净值: {current_price:.4f}
- 收益率: <font color="{profit_color}">{profit_sign}{profit_rate*100:.2f}%</font>
"""
        
        if action:
            content += f"- 建议操作: **{action}**\n"
        
        return self.send_markdown(content)
    
    def send_system_status(
        self,
        status: str,
        message: str,
        details: Dict = None
    ) -> bool:
        """发送系统状态通知
        
        Args:
            status: 状态 (normal/warning/error)
            message: 状态消息
            details: 详细信息
        """
        status_config = {
            "normal": ("✅", "系统正常"),
            "warning": ("⚠️", "系统警告"),
            "error": ("❌", "系统错误"),
        }
        
        emoji, title = status_config.get(status, ("ℹ️", "系统通知"))
        
        content = f"""# {emoji} {title}

{message}
"""
        
        if details:
            content += "\n**详细信息:**\n"
            for key, value in details.items():
                content += f"- {key}: {value}\n"
        
        content += f"\n*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        
        return self.send_markdown(content)
    
    def send_test_message(self) -> bool:
        """发送测试消息"""
        content = f"""# 🔔 测试消息

企业微信机器人配置成功！

**系统信息:**
- 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 状态: <font color="info">正常</font>

您将通过此渠道接收：
- 📊 每日理财报告
- ⚠️ 市场预警通知
- 💰 交易建议提醒
- 🛑 止盈止损提醒

---
*智能理财助手*"""
        
        return self.send_markdown(content)
    
    def _truncate_content(self, content: str, max_bytes: int) -> str:
        """截断内容到指定字节数"""
        encoded = content.encode('utf-8')
        if len(encoded) <= max_bytes:
            return content
        
        # 从后往前找到一个合适的截断点
        truncated = encoded[:max_bytes]
        # 确保不会截断UTF-8字符的中间
        while truncated and (truncated[-1] & 0xC0) == 0x80:
            truncated = truncated[:-1]
        
        return truncated.decode('utf-8', errors='ignore')


# 创建全局实例
wecom_bot = WeComBot()
