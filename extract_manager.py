import re

with open('/Users/wuyang.zhang/.cursor/projects/Users-wuyang-zhang-demo-3-other-fund/agent-tools/38d7e17b-159d-499e-b331-0681ad5ea7de.txt', 'r') as f:
    content = f.read()

match = re.search(r'var Data_currentFundManager\s*=\s*(\[.*?\]);', content, re.DOTALL)
if match:
    print(match.group(1)[:500]) # Print first 500 chars
else:
    print("Not found")
