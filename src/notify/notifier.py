"""
通知推送模块
"""
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from loguru import logger

from config.settings import settings


class Notifier:
    """通知推送器"""
    
    def __init__(self):
        self.email_config = settings.Notification
    
    def send_email(
        self,
        subject: str,
        content: str,
        receiver: str = None,
        is_html: bool = False
    ) -> bool:
        """发送邮件通知
        
        Args:
            subject: 邮件主题
            content: 邮件内容
            receiver: 接收者邮箱，默认使用配置中的
            is_html: 是否为HTML格式
        
        Returns:
            是否发送成功
        """
        receiver = receiver or self.email_config.EMAIL_RECEIVER
        
        if not all([
            self.email_config.SMTP_USER,
            self.email_config.SMTP_PASSWORD,
            receiver
        ]):
            logger.warning("邮件配置不完整，无法发送")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config.SMTP_USER
            msg['To'] = receiver
            msg['Subject'] = subject
            
            content_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(content, content_type, 'utf-8'))
            
            # 连接SMTP服务器
            server = smtplib.SMTP_SSL(
                self.email_config.SMTP_SERVER,
                self.email_config.SMTP_PORT
            )
            server.login(
                self.email_config.SMTP_USER,
                self.email_config.SMTP_PASSWORD
            )
            server.send_message(msg)
            server.quit()
            
            logger.info(f"邮件发送成功: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False
    
    def send_wecom(self, content: str, msg_type: str = "text") -> bool:
        """发送企业微信通知
        
        Args:
            content: 消息内容
            msg_type: 消息类型 (text/markdown)
        
        Returns:
            是否发送成功
        """
        webhook = self.email_config.WECOM_WEBHOOK
        
        if not webhook:
            logger.warning("企业微信Webhook未配置")
            return False
        
        try:
            if msg_type == "markdown":
                data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "content": content
                    }
                }
            else:
                data = {
                    "msgtype": "text",
                    "text": {
                        "content": content
                    }
                }
            
            response = requests.post(webhook, json=data, timeout=10)
            result = response.json()
            
            if result.get("errcode") == 0:
                logger.info("企业微信通知发送成功")
                return True
            else:
                logger.error(f"企业微信通知发送失败: {result}")
                return False
                
        except Exception as e:
            logger.error(f"企业微信通知发送失败: {e}")
            return False
    
    def send_dingtalk(self, content: str, msg_type: str = "text") -> bool:
        """发送钉钉通知
        
        Args:
            content: 消息内容
            msg_type: 消息类型 (text/markdown)
        
        Returns:
            是否发送成功
        """
        webhook = self.email_config.DINGTALK_WEBHOOK
        
        if not webhook:
            logger.warning("钉钉Webhook未配置")
            return False
        
        try:
            if msg_type == "markdown":
                data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": "理财助手通知",
                        "text": content
                    }
                }
            else:
                data = {
                    "msgtype": "text",
                    "text": {
                        "content": content
                    }
                }
            
            response = requests.post(webhook, json=data, timeout=10)
            result = response.json()
            
            if result.get("errcode") == 0:
                logger.info("钉钉通知发送成功")
                return True
            else:
                logger.error(f"钉钉通知发送失败: {result}")
                return False
                
        except Exception as e:
            logger.error(f"钉钉通知发送失败: {e}")
            return False
    
    def send_daily_report(self, report_text: str) -> dict:
        """发送每日报告
        
        尝试通过所有配置的渠道发送
        
        Returns:
            各渠道发送结果
        """
        results = {}
        
        # 邮件
        if self.email_config.EMAIL_RECEIVER:
            results["email"] = self.send_email(
                subject=f"【理财日报】{__import__('datetime').date.today().strftime('%Y-%m-%d')}",
                content=report_text
            )
        
        # 企业微信
        if self.email_config.WECOM_WEBHOOK:
            # 企业微信有长度限制，只发送摘要
            summary = self._extract_summary(report_text)
            results["wecom"] = self.send_wecom(summary, msg_type="text")
        
        # 钉钉
        if self.email_config.DINGTALK_WEBHOOK:
            summary = self._extract_summary(report_text)
            results["dingtalk"] = self.send_dingtalk(summary, msg_type="text")
        
        return results
    
    def send_alert(self, alert_type: str, message: str) -> dict:
        """发送紧急预警
        
        Args:
            alert_type: 预警类型 (risk/opportunity/system)
            message: 预警内容
        """
        type_emoji = {
            "risk": "🚨",
            "opportunity": "💡",
            "system": "⚙️"
        }
        
        emoji = type_emoji.get(alert_type, "📢")
        formatted_message = f"{emoji} 【{alert_type.upper()}预警】\n\n{message}"
        
        results = {}
        
        # 通过所有渠道发送紧急预警
        if self.email_config.EMAIL_RECEIVER:
            results["email"] = self.send_email(
                subject=f"【紧急预警】{alert_type}",
                content=formatted_message
            )
        
        if self.email_config.WECOM_WEBHOOK:
            results["wecom"] = self.send_wecom(formatted_message)
        
        if self.email_config.DINGTALK_WEBHOOK:
            results["dingtalk"] = self.send_dingtalk(formatted_message)
        
        return results
    
    def _extract_summary(self, report_text: str, max_length: int = 2000) -> str:
        """从报告中提取摘要"""
        # 简单截取
        if len(report_text) <= max_length:
            return report_text
        
        # 尝试找到执行摘要部分
        if "执行摘要" in report_text:
            start = report_text.find("执行摘要")
            end = report_text.find("━━━", start + 10)
            if end > start:
                summary = report_text[start:end]
                if len(summary) < max_length:
                    return summary
        
        # 直接截取前面部分
        return report_text[:max_length] + "\n\n... [内容已截断，请查看完整报告]"


# 创建全局实例
notifier = Notifier()
