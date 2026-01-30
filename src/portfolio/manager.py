"""
持仓管理模块
"""
import json
import os
import uuid
from datetime import datetime, date
from typing import Optional, List
from loguru import logger

from ..models import (
    Portfolio, Position, TradeRecord, FundType, 
    SignalType, ConfidenceLevel
)
from config.settings import settings


class PortfolioManager:
    """持仓管理器"""
    
    def __init__(self, portfolio_file: str = None):
        self.portfolio_file = portfolio_file or settings.PORTFOLIO_FILE
        self.trade_history_file = settings.TRADE_HISTORY_FILE
        self.portfolio: Portfolio = self._load_portfolio()
        self.trade_history: List[TradeRecord] = self._load_trade_history()
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.portfolio_file), exist_ok=True)
    
    def _load_portfolio(self) -> Portfolio:
        """加载持仓数据"""
        if os.path.exists(self.portfolio_file):
            try:
                with open(self.portfolio_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return Portfolio(**data)
            except Exception as e:
                logger.error(f"加载持仓数据失败: {e}")
        return Portfolio(cash=0, positions=[], total_value=0)
    
    def _load_trade_history(self) -> List[TradeRecord]:
        """加载交易历史"""
        if os.path.exists(self.trade_history_file):
            try:
                with open(self.trade_history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return [TradeRecord(**record) for record in data]
            except Exception as e:
                logger.error(f"加载交易历史失败: {e}")
        return []
    
    def save_portfolio(self):
        """保存持仓数据"""
        os.makedirs(os.path.dirname(self.portfolio_file), exist_ok=True)
        with open(self.portfolio_file, 'w', encoding='utf-8') as f:
            json.dump(self.portfolio.model_dump(mode='json'), f, ensure_ascii=False, indent=2, default=str)
        logger.info("持仓数据已保存")
    
    def save_trade_history(self):
        """保存交易历史"""
        os.makedirs(os.path.dirname(self.trade_history_file), exist_ok=True)
        with open(self.trade_history_file, 'w', encoding='utf-8') as f:
            records = [r.model_dump(mode='json') for r in self.trade_history]
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
        logger.info("交易历史已保存")
    
    def initialize_portfolio(self, initial_cash: float):
        """初始化投资组合"""
        self.portfolio = Portfolio(
            cash=initial_cash,
            total_value=initial_cash,
            positions=[],
            last_update=datetime.now()
        )
        self.save_portfolio()
        logger.info(f"投资组合已初始化，初始资金: ¥{initial_cash:,.2f}")
    
    def add_position(
        self,
        fund_code: str,
        fund_name: str,
        fund_type: FundType,
        shares: float,
        price: float,
        reason: str = "",
        confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    ) -> bool:
        """添加持仓（买入）"""
        amount = shares * price
        
        # 检查现金是否足够
        if amount > self.portfolio.cash:
            logger.warning(f"现金不足，无法买入 {fund_name}")
            return False
        
        # 检查是否已有该基金持仓
        existing_pos = self.portfolio.get_position(fund_code)
        
        if existing_pos:
            # 加仓，计算新的成本价
            total_shares = existing_pos.shares + shares
            total_cost = existing_pos.shares * existing_pos.cost_price + amount
            new_cost_price = total_cost / total_shares
            
            existing_pos.shares = total_shares
            existing_pos.cost_price = new_cost_price
            existing_pos.update_price(price)
        else:
            # 新建持仓
            new_position = Position(
                fund_code=fund_code,
                fund_name=fund_name,
                fund_type=fund_type,
                shares=shares,
                cost_price=price,
                current_price=price,
                market_value=amount,
                profit_loss=0,
                profit_rate=0,
                buy_date=date.today(),
                hold_days=0
            )
            self.portfolio.positions.append(new_position)
        
        # 扣除现金
        self.portfolio.cash -= amount
        self.portfolio.calculate_total()
        self.portfolio.last_update = datetime.now()
        
        # 记录交易
        trade_record = TradeRecord(
            id=str(uuid.uuid4()),
            fund_code=fund_code,
            fund_name=fund_name,
            trade_type=SignalType.BUY,
            shares=shares,
            price=price,
            amount=amount,
            fee=0,  # 简化处理，暂不计算手续费
            trade_date=datetime.now(),
            reason=reason,
            confidence=confidence
        )
        self.trade_history.append(trade_record)
        
        self.save_portfolio()
        self.save_trade_history()
        
        logger.info(f"买入成功: {fund_name}({fund_code}), 份额: {shares}, 金额: ¥{amount:,.2f}")
        return True
    
    def reduce_position(
        self,
        fund_code: str,
        shares: float,
        price: float,
        reason: str = "",
        confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    ) -> bool:
        """减少持仓（卖出）"""
        position = self.portfolio.get_position(fund_code)
        
        if not position:
            logger.warning(f"未找到基金 {fund_code} 的持仓")
            return False
        
        if shares > position.shares:
            shares = position.shares  # 最多卖出全部
        
        amount = shares * price
        
        # 更新持仓
        position.shares -= shares
        position.update_price(price)
        
        # 如果份额为0，移除持仓
        if position.shares <= 0:
            self.portfolio.positions.remove(position)
        
        # 增加现金
        self.portfolio.cash += amount
        self.portfolio.calculate_total()
        self.portfolio.last_update = datetime.now()
        
        # 记录交易
        trade_record = TradeRecord(
            id=str(uuid.uuid4()),
            fund_code=fund_code,
            fund_name=position.fund_name,
            trade_type=SignalType.SELL,
            shares=shares,
            price=price,
            amount=amount,
            fee=0,
            trade_date=datetime.now(),
            reason=reason,
            confidence=confidence
        )
        self.trade_history.append(trade_record)
        
        self.save_portfolio()
        self.save_trade_history()
        
        logger.info(f"卖出成功: {position.fund_name}({fund_code}), 份额: {shares}, 金额: ¥{amount:,.2f}")
        return True
    
    def update_prices(self, price_dict: dict):
        """更新持仓净值
        
        Args:
            price_dict: {fund_code: current_price}
        """
        for position in self.portfolio.positions:
            if position.fund_code in price_dict:
                position.update_price(price_dict[position.fund_code])
        
        self.portfolio.calculate_total()
        self.portfolio.last_update = datetime.now()
        self.save_portfolio()
        logger.info("持仓净值已更新")
    
    def get_portfolio_summary(self) -> dict:
        """获取持仓概览"""
        total_profit = sum(p.profit_loss for p in self.portfolio.positions)
        total_cost = sum(p.shares * p.cost_price for p in self.portfolio.positions)
        
        return {
            "total_value": self.portfolio.total_value,
            "cash": self.portfolio.cash,
            "position_value": self.portfolio.total_value - self.portfolio.cash,
            "total_profit": total_profit,
            "total_profit_rate": total_profit / total_cost if total_cost > 0 else 0,
            "position_count": len(self.portfolio.positions),
            "position_ratio": self.portfolio.get_total_position_ratio(),
            "last_update": self.portfolio.last_update.isoformat() if self.portfolio.last_update else None
        }
    
    def get_position_details(self) -> List[dict]:
        """获取持仓明细"""
        details = []
        for pos in self.portfolio.positions:
            details.append({
                "fund_code": pos.fund_code,
                "fund_name": pos.fund_name,
                "fund_type": pos.fund_type.value,
                "shares": pos.shares,
                "cost_price": pos.cost_price,
                "current_price": pos.current_price,
                "market_value": pos.market_value,
                "profit_loss": pos.profit_loss,
                "profit_rate": pos.profit_rate,
                "hold_days": pos.hold_days,
                "position_ratio": self.portfolio.get_position_ratio(pos.fund_code)
            })
        return details
    
    def get_today_trades(self) -> List[TradeRecord]:
        """获取今日交易记录"""
        today = date.today()
        return [
            t for t in self.trade_history 
            if t.trade_date.date() == today
        ]
    
    def get_fund_last_buy_date(self, fund_code: str) -> Optional[date]:
        """获取某基金最近一次买入日期"""
        buy_records = [
            t for t in self.trade_history 
            if t.fund_code == fund_code and t.trade_type == SignalType.BUY
        ]
        if buy_records:
            return max(t.trade_date.date() for t in buy_records)
        return None
    
    def check_position_limits(self, fund_code: str, fund_type: FundType, amount: float) -> dict:
        """检查持仓限制
        
        返回: {"allowed": bool, "warnings": list}
        """
        warnings = []
        risk_config = settings.RiskControl
        
        # 检查单只基金仓位
        current_ratio = self.portfolio.get_position_ratio(fund_code)
        new_ratio = current_ratio + (amount / self.portfolio.total_value if self.portfolio.total_value > 0 else 0)
        if new_ratio > risk_config.MAX_SINGLE_FUND_POSITION:
            warnings.append(f"单只基金仓位将超过{risk_config.MAX_SINGLE_FUND_POSITION*100}%上限")
        
        # 检查同类型基金仓位
        category_ratio = self.portfolio.get_category_ratio(fund_type)
        new_category_ratio = category_ratio + (amount / self.portfolio.total_value if self.portfolio.total_value > 0 else 0)
        if new_category_ratio > risk_config.MAX_CATEGORY_POSITION:
            warnings.append(f"同类型基金仓位将超过{risk_config.MAX_CATEGORY_POSITION*100}%上限")
        
        # 检查单次买入金额
        if amount > self.portfolio.cash * risk_config.MAX_SINGLE_BUY_RATIO:
            warnings.append(f"单次买入金额超过可用资金的{risk_config.MAX_SINGLE_BUY_RATIO*100}%")
        
        # 检查每日交易次数
        today_trades = self.get_today_trades()
        if len(today_trades) >= risk_config.MAX_DAILY_TRADES:
            warnings.append(f"今日交易次数已达上限({risk_config.MAX_DAILY_TRADES}次)")
            return {"allowed": False, "warnings": warnings}
        
        # 检查冷静期
        last_buy = self.get_fund_last_buy_date(fund_code)
        if last_buy:
            days_since_last_buy = (date.today() - last_buy).days
            if days_since_last_buy < risk_config.BUY_COOLDOWN_DAYS:
                warnings.append(f"距离上次买入仅{days_since_last_buy}天，建议等待{risk_config.BUY_COOLDOWN_DAYS}天冷静期")
        
        return {
            "allowed": len([w for w in warnings if "上限" in w or "达上限" in w]) == 0,
            "warnings": warnings
        }
