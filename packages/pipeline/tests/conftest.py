import sys
from pathlib import Path

# make synthetic_bundle.py importable from the tests
sys.path.insert(0, str(Path(__file__).resolve().parent))
