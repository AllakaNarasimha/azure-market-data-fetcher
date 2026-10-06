from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds


# ============================================================
# Models
# ============================================================

@dataclass(frozen=True, slots=True)
class OptionContract:
    symbol: str
    option_type: str
    strike_price: float
    ltp: float | None
    bid: float | None
    ask: float | None
    volume: int | float | None
    oi: int | float | None
    expiry_timestamp: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StrikePair:
    strike_price: float
    offset: int
    ce: OptionContract | None
    pe: OptionContract | None

    def to_dict(self) -> dict:
        return {
            "strike_price": self.strike_price,
            "offset": self.offset,
            "ce": self.ce.to_dict() if self.ce else None,
            "pe": self.pe.to_dict() if self.pe else None,
        }


@dataclass(frozen=True, slots=True)
class ExpiryChain:
    expiry_date: date
    expiry_timestamp: int
    is_monthly: bool
    atm_strike: float
    strikes: tuple[StrikePair, ...]

    def to_dict(self) -> dict:
        return {
            "expiry_date": self.expiry_date.isoformat(),
            "expiry_timestamp": self.expiry_timestamp,
            "is_monthly": self.is_monthly,
            "atm_strike": self.atm_strike,
            "strikes": [x.to_dict() for x in self.strikes],
        }


@dataclass(frozen=True, slots=True)
class OptionChainResult:
    requested_time: pd.Timestamp
    snapshot_time: pd.Timestamp
    underlying: str
    spot_price: float
    strike_offset: int
    expiries: tuple[ExpiryChain, ...]

    def to_dict(self) -> dict:
        return {
            "requested_time": self.requested_time.isoformat(),
            "snapshot_time": self.snapshot_time.isoformat(),
            "underlying": self.underlying,
            "spot_price": self.spot_price,
            "strike_offset": self.strike_offset,
            "expiries": [x.to_dict() for x in self.expiries],
        }


# ============================================================
# MD Reader
# ============================================================

class MDReader:
    """
    Fast read-only market-data reader backed by Parquet.

    Expected columns:
        fetched_at
        expiry_timestamp
        strike_price
        option_type
        symbol
        ltp
        bid
        ask
        volume
        oi

    Index/spot row:
        strike_price == -1
        symbol ends with "-INDEX"

    Important:
        - fetched_at identifies a snapshot.
        - duplicated INDEX rows across expiry blocks are logically
          one spot observation.
        - strike spacing is NEVER hard-coded.
    """

    REQUIRED_COLUMNS = (
        "fetched_at",
        "expiry_timestamp",
        "strike_price",
        "option_type",
        "symbol",
        "ltp",
        "bid",
        "ask",
        "volume",
        "oi",
    )

    def __init__(
        self,
        parquet_path: str | Path,
        timezone: str = "Asia/Kolkata",
    ):
        self.parquet_path = Path(parquet_path)
        self.timezone = timezone

        # Works for:
        #   single parquet file
        #   directory of parquet files
        #   Hive partitioned dataset
        self._dataset = ds.dataset(
            str(self.parquet_path),
            format="parquet",
            partitioning="hive",
        )

        self._validate_schema()

    # ========================================================
    # Public API
    # ========================================================

    def get_option_chain(
        self,
        time,
        strike_offset: int = 0,
        weekly_expiry_offset: int | None = 0,
        monthly_expiry_offset: int | None = None,
    ) -> OptionChainResult:
        """
        Get CE + PE around ATM for the requested snapshot.

        Examples:

            strike_offset=0
                ATM only

            strike_offset=1
                ATM-1 and ATM+1

            strike_offset=2
                ATM-2 and ATM+2

        Expiry offsets:

            weekly_expiry_offset=0
                nearest expiry

            weekly_expiry_offset=1
                next expiry

            monthly_expiry_offset=0
                nearest monthly expiry

        Monthly/weekly duplicate expiry dates are returned once.
        """

        self._validate_offsets(
            strike_offset,
            weekly_expiry_offset,
            monthly_expiry_offset,
        )

        requested_time = self._normalize_time(time)

        snapshot = self._load_snapshot(requested_time)

        if snapshot.empty:
            raise LookupError(
                f"No option-chain snapshot available at or before "
                f"{requested_time}"
            )

        snapshot_time = snapshot["fetched_at"].iloc[0]

        # ----------------------------------------------------
        # Spot / underlying
        # ----------------------------------------------------

        index_rows = snapshot[self._index_mask(snapshot)]

        if index_rows.empty:
            raise LookupError(
                f"Underlying/index row missing at {snapshot_time}"
            )

        # Same INDEX row can appear once per expiry.
        # We only need one logical observation.
        index_row = index_rows.iloc[0]

        spot_price = float(index_row["ltp"])
        underlying = str(index_row["symbol"])

        # ----------------------------------------------------
        # Option rows
        # ----------------------------------------------------

        options = snapshot[
            snapshot["option_type"].isin(("CE", "PE"))
        ].copy()

        if options.empty:
            raise LookupError(
                f"No CE/PE rows available at {snapshot_time}"
            )

        options["expiry_timestamp"] = (
            options["expiry_timestamp"].astype("int64")
        )

        options["expiry_date"] = (
            pd.to_datetime(
                options["expiry_timestamp"],
                unit="s",
                utc=True,
            )
            .dt.tz_convert(self.timezone)
            .dt.date
        )

        # ----------------------------------------------------
        # Expiry resolution
        # ----------------------------------------------------

        expiry_dates = sorted(
            options["expiry_date"].dropna().unique()
        )

        monthly_expiries = self._monthly_expiries(expiry_dates)

        selected_expiries = self._select_expiries(
            expiry_dates=expiry_dates,
            monthly_expiries=monthly_expiries,
            weekly_offset=weekly_expiry_offset,
            monthly_offset=monthly_expiry_offset,
        )

        # ----------------------------------------------------
        # Build chains
        # ----------------------------------------------------

        chains: list[ExpiryChain] = []

        for expiry_date in selected_expiries:
            expiry_df = options[
                options["expiry_date"] == expiry_date
            ]

            chain = self._build_expiry_chain(
                expiry_df=expiry_df,
                expiry_date=expiry_date,
                spot_price=spot_price,
                strike_offset=strike_offset,
                is_monthly=expiry_date in monthly_expiries,
            )

            chains.append(chain)

        return OptionChainResult(
            requested_time=requested_time,
            snapshot_time=snapshot_time,
            underlying=underlying,
            spot_price=spot_price,
            strike_offset=strike_offset,
            expiries=tuple(chains),
        )

    # ========================================================
    # Snapshot loading
    # ========================================================

    def _load_snapshot(
        self,
        requested_time: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Locate the latest exact fetched_at <= requested_time.

        We first read timestamps only, then load the selected snapshot.
        This avoids loading the complete option dataset.
        """

        requested_ns = self._arrow_time_value(requested_time)

        # First query: timestamp column only.
        table = self._dataset.to_table(
            columns=["fetched_at"],
            filter=ds.field("fetched_at") <= requested_ns,
        )

        if table.num_rows == 0:
            return pd.DataFrame()

        timestamps = table.column("fetched_at").to_pandas()

        snapshot_time = timestamps.max()

        return self._load_exact_snapshot(snapshot_time)

    @lru_cache(maxsize=128)
    def _load_exact_snapshot(
        self,
        snapshot_time,
    ) -> pd.DataFrame:
        """
        Cached exact snapshot read.

        Helpful because multiple strategy rules may request the
        same option snapshot.
        """

        table = self._dataset.to_table(
            columns=list(self.REQUIRED_COLUMNS),
            filter=ds.field("fetched_at") == snapshot_time,
        )

        df = table.to_pandas()

        if df.empty:
            return df

        df["fetched_at"] = pd.to_datetime(df["fetched_at"])

        return df

    # ========================================================
    # Expiry chain
    # ========================================================

    def _build_expiry_chain(
        self,
        expiry_df: pd.DataFrame,
        expiry_date: date,
        spot_price: float,
        strike_offset: int,
        is_monthly: bool,
    ) -> ExpiryChain:

        if expiry_df.empty:
            raise LookupError(
                f"No options available for expiry {expiry_date}"
            )

        # ----------------------------------------------------
        # Dynamic strikes
        #
        # NO assumptions such as:
        #   NIFTY = 50
        #   BANKNIFTY = 100
        #
        # The actual available strikes determine spacing.
        # ----------------------------------------------------

        strikes = sorted(
            expiry_df["strike_price"]
            .dropna()
            .astype(float)
            .unique()
        )

        if not strikes:
            raise LookupError(
                f"No strikes available for {expiry_date}"
            )

        # ----------------------------------------------------
        # Find actual ATM
        # ----------------------------------------------------

        atm_position = min(
            range(len(strikes)),
            key=lambda i: (
                abs(strikes[i] - spot_price),
                strikes[i],
            ),
        )

        atm_strike = strikes[atm_position]

        selected_positions = self._strike_positions(
            atm_position=atm_position,
            strike_count=len(strikes),
            offset=strike_offset,
        )

        pairs: list[StrikePair] = []

        for position in selected_positions:
            strike = strikes[position]

            strike_df = expiry_df[
                expiry_df["strike_price"].astype(float) == strike
            ]

            ce_row = strike_df[
                strike_df["option_type"] == "CE"
            ]

            pe_row = strike_df[
                strike_df["option_type"] == "PE"
            ]

            pairs.append(
                StrikePair(
                    strike_price=strike,
                    offset=position - atm_position,
                    ce=self._to_contract(ce_row),
                    pe=self._to_contract(pe_row),
                )
            )

        expiry_timestamp = int(
            expiry_df["expiry_timestamp"].iloc[0]
        )

        return ExpiryChain(
            expiry_date=expiry_date,
            expiry_timestamp=expiry_timestamp,
            is_monthly=is_monthly,
            atm_strike=atm_strike,
            strikes=tuple(pairs),
        )

    # ========================================================
    # Strike selection
    # ========================================================

    @staticmethod
    def _strike_positions(
        atm_position: int,
        strike_count: int,
        offset: int,
    ) -> list[int]:

        if offset == 0:
            return [atm_position]

        positions = []

        lower = atm_position - offset
        upper = atm_position + offset

        if lower >= 0:
            positions.append(lower)

        if upper < strike_count:
            positions.append(upper)

        return positions

    # ========================================================
    # Expiry selection
    # ========================================================

    @staticmethod
    def _monthly_expiries(
        expiry_dates: list[date],
    ) -> set[date]:
        """
        Last available expiry in each calendar month is considered
        the monthly expiry.
        """

        months: dict[tuple[int, int], date] = {}

        for expiry in expiry_dates:
            key = (expiry.year, expiry.month)

            current = months.get(key)

            if current is None or expiry > current:
                months[key] = expiry

        return set(months.values())

    @staticmethod
    def _select_expiries(
        expiry_dates: list[date],
        monthly_expiries: set[date],
        weekly_offset: int | None,
        monthly_offset: int | None,
    ) -> list[date]:

        selected: list[date] = []

        # All chronological expiries form the weekly sequence.
        if weekly_offset is not None:
            if weekly_offset >= len(expiry_dates):
                raise IndexError(
                    f"Weekly expiry offset {weekly_offset} unavailable; "
                    f"only {len(expiry_dates)} expiries found."
                )

            selected.append(expiry_dates[weekly_offset])

        if monthly_offset is not None:
            monthly = sorted(monthly_expiries)

            if monthly_offset >= len(monthly):
                raise IndexError(
                    f"Monthly expiry offset {monthly_offset} unavailable; "
                    f"only {len(monthly)} monthly expiries found."
                )

            selected.append(monthly[monthly_offset])

        # Preserve order while removing duplicates.
        return list(dict.fromkeys(selected))

    # ========================================================
    # Conversion
    # ========================================================

    @staticmethod
    def _to_contract(
        rows: pd.DataFrame,
    ) -> OptionContract | None:

        if rows.empty:
            return None

        row = rows.iloc[0]

        return OptionContract(
            symbol=str(row["symbol"]),
            option_type=str(row["option_type"]),
            strike_price=float(row["strike_price"]),
            ltp=MDReader._number(row.get("ltp")),
            bid=MDReader._number(row.get("bid")),
            ask=MDReader._number(row.get("ask")),
            volume=MDReader._number(row.get("volume")),
            oi=MDReader._number(row.get("oi")),
            expiry_timestamp=int(row["expiry_timestamp"]),
        )

    @staticmethod
    def _number(value) -> Any:
        if value is None or pd.isna(value):
            return None

        if hasattr(value, "item"):
            return value.item()

        return value

    # ========================================================
    # Validation / utilities
    # ========================================================

    def _validate_schema(self):
        available = set(self._dataset.schema.names)
        missing = set(self.REQUIRED_COLUMNS) - available

        if missing:
            raise ValueError(
                f"Missing parquet columns: {sorted(missing)}"
            )

    @staticmethod
    def _validate_offsets(
        strike_offset,
        weekly_offset,
        monthly_offset,
    ):
        if strike_offset < 0:
            raise ValueError("strike_offset must be >= 0")

        if weekly_offset is not None and weekly_offset < 0:
            raise ValueError(
                "weekly_expiry_offset must be >= 0 or None"
            )

        if monthly_offset is not None and monthly_offset < 0:
            raise ValueError(
                "monthly_expiry_offset must be >= 0 or None"
            )

    def _normalize_time(self, value) -> pd.Timestamp:
        ts = pd.Timestamp(value)

        if ts.tzinfo is None:
            ts = ts.tz_localize(self.timezone)

        return ts

    @staticmethod
    def _index_mask(df: pd.DataFrame) -> pd.Series:
        return (
            df["strike_price"].eq(-1)
            & df["symbol"]
            .astype(str)
            .str.endswith("-INDEX")
        )

    @staticmethod
    def _arrow_time_value(ts: pd.Timestamp):
        """
        Adjust this if fetched_at in your parquet is stored as
        UTC-naive timestamp rather than timezone-aware timestamp.
        """
        return ts

# ============================================================
# Main / Local validation
# ============================================================

def main():
    PARQUET_PATH = "data/NSE_NIFTY50-INDEX/"
    TEST_TIME = "2026-10-06 10:30:00"

    print("=" * 80)
    print("MDReader Option Chain Validation")
    print("=" * 80)

    reader = MDReader(PARQUET_PATH)

    # --------------------------------------------------------
    # Helper
    # --------------------------------------------------------

    def run_case(
        name: str,
        *,
        time=TEST_TIME,
        strike_offset=0,
        weekly_expiry_offset=0,
        monthly_expiry_offset=None,
    ):
        print()
        print("-" * 80)
        print(f"TEST: {name}")
        print("-" * 80)

        try:
            result = reader.get_option_chain(
                time=time,
                strike_offset=strike_offset,
                weekly_expiry_offset=weekly_expiry_offset,
                monthly_expiry_offset=monthly_expiry_offset,
            )

            print(f"Requested Time : {result.requested_time}")
            print(f"Snapshot Time  : {result.snapshot_time}")
            print(f"Underlying     : {result.underlying}")
            print(f"Spot Price     : {result.spot_price}")
            print(f"Strike Offset  : {result.strike_offset}")

            for expiry in result.expiries:
                print()
                print(
                    f"Expiry          : {expiry.expiry_date} "
                    f"{'(MONTHLY)' if expiry.is_monthly else '(WEEKLY)'}"
                )

                print(f"ATM Strike      : {expiry.atm_strike}")

                for pair in expiry.strikes:
                    ce_ltp = pair.ce.ltp if pair.ce else None
                    pe_ltp = pair.pe.ltp if pair.pe else None

                    print(
                        f"  offset={pair.offset:+d} "
                        f"strike={pair.strike_price:<10} "
                        f"CE={ce_ltp!s:<10} "
                        f"PE={pe_ltp!s:<10}"
                    )

            return result

        except Exception as exc:
            print(
                f"{type(exc).__name__}: {exc}"
            )

            return None

    # ========================================================
    # Scenario 1
    #
    # ATM only + nearest weekly expiry
    # ========================================================

    run_case(
        "ATM / nearest weekly",
        strike_offset=0,
        weekly_expiry_offset=0,
        monthly_expiry_offset=None,
    )
