from src.collector.eastmoney_collector import EastMoneyCollector
import json

collector = EastMoneyCollector()
# 000001
detail = collector.get_fund_detail("000001")
print("=== Fund Detail Keys ===")
print(list(detail.keys()))

if 'managers' in detail:
    print("\n=== Managers Data ===")
    print(json.dumps(detail['managers'], indent=2, ensure_ascii=False))
else:
    print("\nNo managers data found")
