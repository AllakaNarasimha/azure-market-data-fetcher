import os
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
