import akshare as ak
import pandas as pd

print("=== Searching for fund manager functions ===")
for attr in dir(ak):
    if "fund" in attr and "manager" in attr:
        print(attr)

print("\n=== Testing fund holdings ===")
try:
    # 000001
    holdings = ak.fund_portfolio_hold_em(symbol="000001", date="2025") # Try 2025 as it's 2026 now
    print("Columns:", holdings.columns.tolist())
    print(holdings.head(3).to_string())
except Exception as e:
    print(f"Error 2025: {e}")
    try:
        holdings = ak.fund_portfolio_hold_em(symbol="000001", date="2024")
        print("Columns (2024):", holdings.columns.tolist())
        print(holdings.head(3).to_string())
    except Exception as e:
         print(f"Error 2024: {e}")
