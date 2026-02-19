import akshare as ak
import inspect

print("=== Help for fund_manager_em ===")
try:
    print(inspect.signature(ak.fund_manager_em))
except Exception as e:
    print(e)

try:
    print(ak.fund_manager_em.__doc__)
except:
    pass
