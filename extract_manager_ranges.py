import re

with open('/Users/wuyang.zhang/.cursor/projects/Users-wuyang-zhang-demo-3-other-fund/agent-tools/38d7e17b-159d-499e-b331-0681ad5ea7de.txt', 'r') as f:
    content = f.read()

# Look for manager ranges
match = re.search(r'var Data_fundManagerRanges\s*=\s*(\{.*?\});', content, re.DOTALL)
if match:
    print("Found Data_fundManagerRanges")
    print(match.group(1)[:500])
else:
    print("Data_fundManagerRanges not found")
