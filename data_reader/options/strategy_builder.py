from __future__ import annotations

from .models import (
    Side,
    StrategyLeg,
    StrategyResult,
    StrategyType,
)

from .option_selector import (
    OptionSelector,
    SelectedContract,
)

from .strategy_config import (
    DEFAULT_OPTION_STRATEGY_CONFIG,
    OptionStrategyConfig,
)


# ============================================================
# Sentinel
#
# _UNSET means:
#     caller did not provide the parameter
#     -> use configured default
#
# None means:
#     caller explicitly disabled that selection
#
# Example:
#
# weekly_expiry_offset=_UNSET
#     -> use config default (usually 0)
#
# weekly_expiry_offset=None
#     -> do NOT select weekly expiry
# ============================================================

_UNSET = object()


class OptionStrategyBuilder:

    def __init__(
        self,
        reader,
        config: OptionStrategyConfig | None = None,
    ):
        self.reader = reader
        self.selector = OptionSelector(reader)

        self.config = (
            config
            or DEFAULT_OPTION_STRATEGY_CONFIG
        )

    # ========================================================
    # STRADDLE
    # ========================================================

    def straddle(
        self,
        time,
        side: Side,
        strike_offset=_UNSET,
        weekly_expiry_offset=_UNSET,
        monthly_expiry_offset=_UNSET,
    ) -> StrategyResult:

        config = self.config.straddle

        strike_offset = self._resolve(
            strike_offset,
            config.strike_offset,
        )

        weekly_expiry_offset = self._resolve(
            weekly_expiry_offset,
            config.weekly_expiry_offset,
        )

        monthly_expiry_offset = self._resolve(
            monthly_expiry_offset,
            config.monthly_expiry_offset,
        )

        if strike_offset != 0:
            raise ValueError(
                "Straddle requires strike_offset=0."
            )

        pair = self.selector.pair(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        chain = self.reader.get_option_chain(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        return StrategyResult(
            strategy=StrategyType.STRADDLE,
            side=side,
            underlying=chain.underlying,
            spot_price=chain.spot_price,
            requested_time=chain.requested_time,
            snapshot_time=chain.snapshot_time,
            legs=(
                self._leg(side, pair.ce),
                self._leg(side, pair.pe),
            ),
        )

    # ========================================================
    # STRANGLE
    # ========================================================

    def strangle(
        self,
        time,
        side: Side,
        strike_offset=_UNSET,
        weekly_expiry_offset=_UNSET,
        monthly_expiry_offset=_UNSET,
    ) -> StrategyResult:

        config = self.config.strangle

        strike_offset = self._resolve(
            strike_offset,
            config.strike_offset,
        )

        weekly_expiry_offset = self._resolve(
            weekly_expiry_offset,
            config.weekly_expiry_offset,
        )

        monthly_expiry_offset = self._resolve(
            monthly_expiry_offset,
            config.monthly_expiry_offset,
        )

        if strike_offset <= 0:
            raise ValueError(
                "Strangle strike_offset must be > 0."
            )

        selected = self.selector.strangle(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        chain = self.reader.get_option_chain(
            time=time,
            strike_offset=strike_offset,
            weekly_expiry_offset=weekly_expiry_offset,
            monthly_expiry_offset=monthly_expiry_offset,
        )

        return StrategyResult(
            strategy=StrategyType.STRANGLE,
            side=side,
            underlying=chain.underlying,
            spot_price=chain.spot_price,
            requested_time=chain.requested_time,
            snapshot_time=chain.snapshot_time,
            legs=(
                self._leg(side, selected.put),
                self._leg(side, selected.call),
            ),
        )

    # ========================================================
    # Convenience API
    # ========================================================

    def short_straddle(
        self,
        time,
        **kwargs,
    ) -> StrategyResult:

        return self.straddle(
            time=time,
            side=Side.SELL,
            **kwargs,
        )

    def long_straddle(
        self,
        time,
        **kwargs,
    ) -> StrategyResult:

        return self.straddle(
            time=time,
            side=Side.BUY,
            **kwargs,
        )

    def short_strangle(
        self,
        time,
        **kwargs,
    ) -> StrategyResult:

        return self.strangle(
            time=time,
            side=Side.SELL,
            **kwargs,
        )

    def long_strangle(
        self,
        time,
        **kwargs,
    ) -> StrategyResult:

        return self.strangle(
            time=time,
            side=Side.BUY,
            **kwargs,
        )

    # ========================================================
    # Helpers
    # ========================================================

    @staticmethod
    def _resolve(value, default):
        """
        Resolve an optional strategy parameter.

        _UNSET:
            caller omitted parameter
            -> use configured default

        None:
            caller explicitly supplied None
            -> preserve None

        Any other value:
            caller override
            -> use supplied value
        """
        return default if value is _UNSET else value

    @staticmethod
    def _leg(
        side: Side,
        contract: SelectedContract,
    ) -> StrategyLeg:

        return StrategyLeg(
            side=side,
            option_type=contract.option_type,
            symbol=contract.symbol,
            strike_price=contract.strike_price,
            expiry_timestamp=contract.expiry_timestamp,
            ltp=contract.ltp,
            bid=contract.bid,
            ask=contract.ask,
        )
