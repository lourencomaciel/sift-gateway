from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
UNIT = ROOT / "tests" / "unit"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(UNIT) not in sys.path:
    sys.path.insert(0, str(UNIT))
