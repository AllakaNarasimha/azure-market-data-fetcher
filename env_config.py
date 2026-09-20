import os
import logging
from datetime import datetime, timedelta
from typing import List, Optional


class EnvConfig:
    """Centralized environment variable access with runtime overrides for tests.

    Usage:
      - Read a variable: `EnvConfig.env('MY_VAR', 'default')`
      - Typed helpers: `EnvConfig.test_mode()`, `EnvConfig.option_chain_symbols()`
      - Tests can call `EnvConfig.set_override('MY_VAR', 'value')` to avoid
        touching the process environment in unit tests.
    """

    _overrides: dict = {}
    # TEST_MODE window tracking (initialized on first call)
    _test_mode_initialized: bool = False
    _TEST_MODE_START: Optional[datetime] = None
    _TEST_MODE_EXPIRY: Optional[datetime] = None
    _test_mode_completed_logged: bool = False

    @classmethod
    def set_override(cls, key: str, value: Optional[str]) -> None:
        if value is None:
            cls._overrides.pop(key, None)
        else:
            cls._overrides[key] = value

    @classmethod
    def clear_overrides(cls) -> None:
        cls._overrides.clear()

    @classmethod
    def env(cls, key: str, default: Optional[str] = None) -> Optional[str]:
        if key in cls._overrides:
            return cls._overrides.get(key)
        return os.getenv(key, default)

    @classmethod
    def test_mode(cls) -> bool:
        return (cls.env("TEST_MODE", "false") or "").strip().lower() == "true"

    @classmethod
    def test_mode_minutes(cls) -> int:
        raw = cls.env("TEST_MODE_MINUTES", "5") or "5"
        try:
            return int(raw)
        except Exception:
            return 5

    @classmethod
    def option_chain_symbols(cls) -> List[str]:
        raw = cls.env("OPTION_CHAIN_SYMBOLS", "") or ""
        return [s.strip() for s in raw.split(",") if s.strip()]

    @classmethod
    def watchlist_symbols(cls) -> List[str]:
        raw = cls.env("WATCHLIST_SYMBOLS", "SBIN") or "SBIN"
        return [s.strip() for s in raw.split(",") if s.strip()]

    @classmethod
    def prefer_brokers(cls) -> List[str]:
        raw = cls.env("PREFER_BROKERS", "Dhan,Fyers") or "Dhan,Fyers"
        return [b.strip().lower() for b in raw.split(",") if b.strip()]

    @classmethod
    def website_site_name(cls) -> Optional[str]:
        return cls.env("WEBSITE_SITE_NAME")

    @classmethod
    def market_storage_connection(cls) -> Optional[str]:
        return cls.env("MARKET_STORAGE_CONNECTION")

    @classmethod
    def key_vault_url(cls) -> Optional[str]:
        return cls.env("KEY_VAULT_URL")

    @classmethod
    def init_test_mode_window(cls, now: datetime) -> None:
        """Initialize a short-lived TEST_MODE startup window at `now`.

        This is idempotent and safe to call multiple times; the window is
        established only once per process.
        """
        if not cls.test_mode():
            return
        # Always (re)initialize the test-mode window when requested. Tests
        # reload the module but `EnvConfig` lives in a separate module and
        # retains class state across reloads; resetting here ensures tests
        # get a fresh window and that the "completed" log flag is cleared.
        minutes = cls.test_mode_minutes()
        cls._TEST_MODE_START = now
        cls._TEST_MODE_EXPIRY = now + timedelta(minutes=minutes)
        cls._test_mode_initialized = True
        cls._test_mode_completed_logged = False
        logging.info("TEST_MODE enabled: running for %s minutes until %s", minutes, cls._TEST_MODE_EXPIRY.isoformat())

    @classmethod
    def is_test_mode_active(cls, now: datetime) -> bool:
        """Return True when TEST_MODE is enabled and still within the startup window.

        Mirrors the previous module-level `_is_test_mode_active` behaviour but
        keeps state inside `EnvConfig` so callers can remain lightweight.
        """
        if not cls.test_mode():
            return False
        if cls._TEST_MODE_EXPIRY is None:
            return False
        if now <= cls._TEST_MODE_EXPIRY:
            return True
        if not cls._test_mode_completed_logged:
            logging.info(
                "TEST_MODE window completed: ran for %s minutes, expired at %s",
                cls.test_mode_minutes(), cls._TEST_MODE_EXPIRY.isoformat(),
            )
            cls._test_mode_completed_logged = True
        return False

    @classmethod
    def get_test_mode_expiry(cls) -> Optional[datetime]:
        return cls._TEST_MODE_EXPIRY
