from __future__ import annotations

import os
from datetime import datetime
import pytz
from typing import Callable, Optional, Tuple
import tempfile


def should_use_test_cache(test_mode: bool, now: Optional[datetime] = None, is_holiday: Optional[Callable[[datetime], bool]] = None, is_local: bool = True) -> Tuple[bool, Optional[str]]:
    """Decide whether to route writes to the test-mode cache.

    Returns (use_test_cache, test_cache_root_or_None).
    - `test_mode`: whether TEST_MODE is enabled
    - `now`: current datetime in Asia/Kolkata (if None, uses now())
    - `is_holiday`: callable taking a datetime and returning True if holiday
    """
    tz = pytz.timezone("Asia/Kolkata")
    now = now or datetime.now(tz)

    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    market_closed = not (market_open <= now <= market_close)
    market_holiday = is_holiday(now) if is_holiday is not None else False

    use_test_cache = test_mode and (market_closed or market_holiday)
    if use_test_cache:
        # Default test cache path under project-local data for local runs
        test_cache_root = os.path.join("local_data", "test_mode_data", "market_data_cache")
        # When running on Azure (not local), use a writable temp directory to avoid PermissionError
        if not is_local:
            test_cache_root = os.path.join(tempfile.gettempdir(), "test_mode_data", "market_data_cache")
        return True, test_cache_root
    return False, None
