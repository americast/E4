"""The test suite. Run from the repository root: python -m unittest discover -s tests -t ."""
import sys
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE) not in sys.path:
    sys.path.insert(0, str(SOURCE))
