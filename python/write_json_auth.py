import json
from pathlib import Path

path = "./auth/dfe-dashboard-analytics-fd61afbed470.json"
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)   # Python dict or list

