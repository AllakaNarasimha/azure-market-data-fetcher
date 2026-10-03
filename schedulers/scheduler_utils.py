from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable

from utils.cache_utils import should_use_test_cache
from utils.env_config import EnvConfig


@dataclass
class SchedulerContext:
    now: datetime
    test_mode_active: bool
    use_test_cache: bool
    test_cache_root: Optional[str]


def build_scheduler_context(now: datetime, is_holiday: Callable[[datetime], bool], is_local: bool) -> SchedulerContext:
    """Build a scheduler context.

    Parameters:
    - now: timezone-aware current datetime (caller provides IST-aware now)
    - is_holiday: callable accepting a datetime returning bool
    - is_local: whether running locally (BlobUtils.is_running_locally())
    """
    test_mode_active = EnvConfig.is_test_mode_active(now)

    use_test_cache, test_cache_root = should_use_test_cache(
        test_mode_active, now=now, is_holiday=is_holiday, is_local=is_local
    )

    return SchedulerContext(now=now, test_mode_active=test_mode_active, use_test_cache=use_test_cache, test_cache_root=test_cache_root)


def calculate_dynamic_option_chain_cron(
    symbol_count: int, batch_size: int, weekday_field: str = "1-5", hour_field: str = "9-16"
) -> Optional[str]:
    if symbol_count == 0:
        return None
    required_seconds = __import__("math").ceil(symbol_count / batch_size)
    end_second = required_seconds - 1
    seconds_field = "0" if end_second == 0 else f"0-{end_second}"
    return f"{seconds_field} * {hour_field} * * {weekday_field}"
