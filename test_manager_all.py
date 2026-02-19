import akshare as ak
import pandas as pd

print("=== Testing fund_manager_em() ===")
try:
    df = ak.fund_manager_em()
    print("Columns:", df.columns.tolist())
    print(df.head(3).to_string())
except Exception as e:
    print(e)
