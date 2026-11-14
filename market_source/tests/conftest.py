# 让 pytest 以 market_source/ 为根导入 app 包
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
