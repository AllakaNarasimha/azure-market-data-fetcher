from __future__ import annotations

import os
from datetime import datetime
import pytz
from typing import Callable, Optional, Tuple
from pathlib import Path
import tempfile

TEST_CACHE_DIRNAME = "test_mode_data"
MARKET_CACHE_DIRNAME = "market_data_cache"

# Azure Blob container names allow only lowercase letters, numbers, and hyphens.
TEST_CONTAINER_NAME = TEST_CACHE_DIRNAME.replace("_", "-")


def is_test_cache_path(local_path: str) -> bool:
    """True if `local_path` lives under the test-mode cache folder."""
    return TEST_CACHE_DIRNAME in str(local_path).replace("\\", "/").lower()


def is_test_blob_path(local_path: str, is_local: bool) -> bool:
    """True if `local_path` should be routed to the test-mode blob prefix.

    Requires TEST_MODE enabled, `local_path` under the test-mode cache folder,
    and `is_local` False - blob sync is a no-op when running locally, so the
    prefix only ever applies to a deployed (non-local) run.
    """
    test_mode = os.getenv("TEST_MODE", "").strip().lower() == "true"
    return test_mode and not is_local and is_test_cache_path(local_path)


def test_blob_name(blob_name: str) -> str:
    """Prefix `blob_name` with the market_data_cache folder for the test-mode container."""
    return f"{MARKET_CACHE_DIRNAME}/{blob_name}"


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
    market_close = now.replace(hour=16, minute=00, second=0, microsecond=0)

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
        canonical = Path(MARKET_CACHE_DIRNAME)
        parent = canonical.parent if canonical.parent != Path("") else Path(".")

        if is_local:
            test_cache_root = str(parent / TEST_CACHE_DIRNAME / MARKET_CACHE_DIRNAME)
        else:
            # Use a writable temp dir on non-local hosts but keep the same
            # sibling structure semantics under the temp dir.
            test_cache_root = str(Path(tempfile.gettempdir()) / TEST_CACHE_DIRNAME / MARKET_CACHE_DIRNAME)

        return True, test_cache_root
    return False, None
