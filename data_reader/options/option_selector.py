from __future__ import annotations

from dataclasses import dataclass

from .models import OptionType


@dataclass(frozen=True, slots=True)
class SelectedContract:
    option_type: OptionType

    symbol: str
    strike_price: float
    expiry_timestamp: int

    ltp: float | None
    bid: float | None
    ask: float | None


@dataclass(frozen=True, slots=True)
class SelectedPair:
    """
    CE + PE at the same strike.
    """
    strike_price: float

    ce: SelectedContract
    pe: SelectedContract


@dataclass(frozen=True, slots=True)
class SelectedStrangle:
    """
    Standard strangle:

        lower strike -> PE
        upper strike -> CE
    """
    put: SelectedContract
    call: SelectedContract


class OptionSelector:
    """
    Responsible only for converting option-chain data into
    strategy-friendly contract selections.

    MDReader remains responsible for:
        - parquet access
        - snapshot selection
        - spot price
        - expiry resolution
        - dynamic strike discovery
    """

    def __init__(self, reader):
        self.reader = reader

    # ========================================================
    # Same-strike CE + PE
    # ========================================================

    def pair(
        self,
        time,
        strike_offset: int = 0,
        weekly_expiry_offset: int | None = 0,
        monthly_expiry_offset: int | None = None,
    ) -> SelectedPair:

        chain = self.reader.get_option_chain(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        expiry = self._single_expiry(chain)

        if len(expiry.strikes) != 1:
            raise LookupError(
                "Expected exactly one strike pair. "
                f"Received {len(expiry.strikes)}."
            )

        pair = expiry.strikes[0]

        if pair.ce is None:
            raise LookupError(
                f"CE missing for strike {pair.strike_price}"
            )

        if pair.pe is None:
            raise LookupError(
                f"PE missing for strike {pair.strike_price}"
            )

        return SelectedPair(
            strike_price=pair.strike_price,
            ce=self._contract(pair.ce),
            pe=self._contract(pair.pe),
        )

    # ========================================================
    # Strangle selection
    # ========================================================

    def strangle(
        self,
        time,
        strike_offset: int = 1,
        weekly_expiry_offset: int | None = 0,
        monthly_expiry_offset: int | None = None,
    ) -> SelectedStrangle:

        if strike_offset <= 0:
            raise ValueError(
                "Strangle strike_offset must be > 0"
            )

        chain = self.reader.get_option_chain(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        expiry = self._single_expiry(chain)

        if len(expiry.strikes) != 2:
            raise LookupError(
                "Strangle requires strikes on both sides of ATM."
            )

        strikes = sorted(
            expiry.strikes,
            key=lambda item: item.strike_price,
        )

        lower = strikes[0]
        upper = strikes[1]

        if lower.pe is None:
            raise LookupError(
                f"PE missing at lower strike "
                f"{lower.strike_price}"
            )

        if upper.ce is None:
            raise LookupError(
                f"CE missing at upper strike "
                f"{upper.strike_price}"
            )

        return SelectedStrangle(
            put=self._contract(lower.pe),
            call=self._contract(upper.ce),
        )

    # ========================================================
    # Single option
    #
    # Useful later for directional strategies/spreads.
    # ========================================================

    def option(
        self,
        time,
        option_type: OptionType,
        strike_offset: int = 0,
        weekly_expiry_offset: int | None = 0,
        monthly_expiry_offset: int | None = None,
    ) -> SelectedContract:

        chain = self.reader.get_option_chain(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        expiry = self._single_expiry(chain)

        if len(expiry.strikes) != 1:
            raise LookupError(
                "Single option selection requires "
                "exactly one strike."
            )

        pair = expiry.strikes[0]

        contract = (
            pair.ce
            if option_type == OptionType.CE
            else pair.pe
        )

        if contract is None:
            raise LookupError(
                f"{option_type.value} unavailable at "
                f"{pair.strike_price}"
            )

        return self._contract(contract)

    # ========================================================
    # Helpers
    # ========================================================

    @staticmethod
    def _single_expiry(chain):
        if not chain.expiries:
            raise LookupError(
                "No expiry available."
            )

        if len(chain.expiries) > 1:
            raise ValueError(
                "Selection resolved multiple expiries. "
                "A strategy leg must use one expiry."
            )

        return chain.expiries[0]

    @staticmethod
    def _contract(contract) -> SelectedContract:
        return SelectedContract(
            option_type=OptionType(
                contract.option_type
            ),
            symbol=contract.symbol,
            strike_price=contract.strike_price,
            expiry_timestamp=contract.expiry_timestamp,
            ltp=contract.ltp,
            bid=contract.bid,
            ask=contract.ask,
        )
