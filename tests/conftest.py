"""
Pytest config — adds the repo root to ``sys.path`` so tests can ``import
recent_sales_parser`` without packaging the module.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
