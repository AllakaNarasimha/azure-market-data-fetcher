from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExpiryConfig:
    weekly_expiry_offset: int | None = 0
    monthly_expiry_offset: int | None = None


@dataclass(frozen=True, slots=True)
class StraddleConfig:
    strike_offset: int = 0

    weekly_expiry_offset: int | None = 0
    monthly_expiry_offset: int | None = None


@dataclass(frozen=True, slots=True)
class StrangleConfig:
    # ATM +/- 1 by default.
    strike_offset: int = 1

    weekly_expiry_offset: int | None = 0
    monthly_expiry_offset: int | None = None


@dataclass(frozen=True, slots=True)
class OptionStrategyConfig:
    straddle: StraddleConfig = StraddleConfig()
    strangle: StrangleConfig = StrangleConfig()


DEFAULT_OPTION_STRATEGY_CONFIG = OptionStrategyConfig()
