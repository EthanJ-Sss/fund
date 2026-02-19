import akshare as ak
import pandas as pd
from datetime import datetime

print("=== 测试基金经理数据 ===")
try:
    # 000001 华夏成长
    managers = ak.fund_manager_em(symbol="000001")
    print("Columns:", managers.columns.tolist())
    print(managers.head(3).to_string())
except Exception as e:
    print(f"Error fetching managers: {e}")

print("\n=== 测试指数历史数据 (沪深300) ===")
try:
    # sh000300
    index_data = ak.stock_zh_index_daily(symbol="sh000300")
    print("Columns:", index_data.columns.tolist())
    print(index_data.tail(3).to_string())
except Exception as e:
    print(f"Error fetching index data: {e}")
