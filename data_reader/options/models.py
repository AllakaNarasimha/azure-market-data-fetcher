from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


class Moneyness(str, Enum):
    ATM = "ATM"
    ITM = "ITM"
    OTM = "OTM"


class StrategyType(str, Enum):
    STRADDLE = "STRADDLE"
    STRANGLE = "STRANGLE"


@dataclass(frozen=True, slots=True)
class StrategyLeg:
    side: Side
    option_type: OptionType
    symbol: str
    strike_price: float
    expiry_timestamp: int

    ltp: float | None = None
    bid: float | None = None
    ask: float | None = None

    quantity: int = 1

    @property
    def entry_price(self) -> float | None:
        """
        More realistic executable price.

        BUY  -> ask
        SELL -> bid

        Falls back to LTP when bid/ask isn't available.
        """
        if self.side == Side.BUY:
            return self.ask if self.ask is not None else self.ltp

        return self.bid if self.bid is not None else self.ltp

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)

        result["side"] = self.side.value
        result["option_type"] = self.option_type.value
        result["entry_price"] = self.entry_price

        return result


@dataclass(frozen=True, slots=True)
class StrategyResult:
    strategy: StrategyType
    side: Side

    underlying: str
    spot_price: float

    requested_time: Any
    snapshot_time: Any

    legs: tuple[StrategyLeg, ...]

    @property
    def total_premium(self) -> float | None:
        """
        Sum of executable leg premiums.

        This is premium points, not final P&L.
        """
        prices = [
            leg.entry_price
            for leg in self.legs
        ]

        if any(price is None for price in prices):
            return None

        return sum(prices)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "side": self.side.value,
            "underlying": self.underlying,
            "spot_price": self.spot_price,
            "requested_time": str(self.requested_time),
            "snapshot_time": str(self.snapshot_time),
            "total_premium": self.total_premium,
            "legs": [
                leg.to_dict()
                for leg in self.legs
            ],
        }
