# setup path so we can import "quantikit"
import sys
from pathlib import Path
d = Path().resolve().parent.parent
sys.path.insert(0, str(d))