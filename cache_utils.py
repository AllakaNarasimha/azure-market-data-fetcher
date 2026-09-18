from __future__ import annotations

import os
from datetime import datetime
import pytz
from typing import Callable, Optional, Tuple
from pathlib import Path
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
        # Place the test_mode_data folder as a sibling to the canonical
        # `market_data_cache` root. This yields e.g. `test_mode_data/market_data_cache`
        # alongside the existing `market_data_cache` directory instead of nested inside it.
        # Determine the canonical market cache root's parent. Prefer a repo-relative
        # `market_data_cache` sibling layout so test artifacts appear alongside the
        # production cache folder instead of nested under it.
        canonical = Path("market_data_cache")
        parent = canonical.parent if canonical.parent != Path("") else Path(".")

        if is_local:
            test_cache_root = str(parent / "test_mode_data" / "market_data_cache")
        else:
            # Use a writable temp dir on non-local hosts but keep the same
            # sibling structure semantics under the temp dir.
            test_cache_root = str(Path(tempfile.gettempdir()) / "test_mode_data" / "market_data_cache")

        return True, test_cache_root
    return False, None
