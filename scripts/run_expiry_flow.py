#!/usr/bin/env python3
"""Runner to exercise DailyScheduler -> prefetch expiries -> LiveScheduler flow locally.

This script enables TEST_MODE briefly to allow scheduler runs outside market hours.
"""
import os
import sys
import importlib
import logging
from datetime import datetime

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

# Ensure TEST_MODE is on so scheduled jobs can run on startup
os.environ.setdefault("TEST_MODE", "true")
os.environ.setdefault("TEST_MODE_MINUTES", "10")

# Import the module
import function_app as fa
importlib.reload(fa)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

print("Running DailyScheduler.run() to prefetch expiries...")
fa.DailyScheduler().run()
print("EXPIRIES_CACHE after DailyScheduler:")
for k, v in list(fa.EXPIRIES_CACHE.items())[:10]:
    print(k, "->", len(v))

print("Running LiveScheduler.run() to exercise live flow (will reuse prefetch if present)...")
fa.LiveScheduler().run()

print("EXPIRIES_CACHE after LiveScheduler (sample):")
for k, v in list(fa.EXPIRIES_CACHE.items())[:10]:
    print(k, "->", len(v))

print("Done.")
