from __future__ import annotations

import urllib.parse
from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# =====================================================================
# DESIGN NOTES
#
# Reviewed response shapes (Fyers, captured via broker_data_manager.main()):
#
# Quote (`quotes()` / get_price/get_quotes):
#   {"d": [{"n": "NSE:SBIN-EQ", "v": {"lp":..., "bid":..., "ask":..., "open_price":...,
#            "high_price":..., "low_price":..., "prev_close_price":..., "volume":...,
#            "tt": "<epoch seconds>", "fyToken":..., ...}, "s": "ok"}], "s": "ok"}
#   -> Flat scalar dict per symbol under "v" - one row per (symbol, snapshot) is a
#      natural fit. Fetched frequently (per price-poll), so partition granularity is
#      per-day, not per-month like candle data.
#
# Option chain (`optionchain()` / get_option_chain):
#   {"data": {"callOi":..., "putOi":..., "expiryData": [{"date":..., "expiry":...,
#            "expiry_flag":...}, ...], "optionsChain": [
#              {"symbol": "NSE:SBIN-EQ", "option_type": "", "strike_price": -1, ...},   # underlying row
#              {"symbol": "NSE:SBIN26SEP980PE", "option_type": "PE", "strike_price": 980,
#               "oi":..., "oich":..., "ltp":..., ...},
#              ...
#            ]}}
#   -> `optionsChain` is already a flat list of per-contract dicts (including one
#      underlying row with option_type=""), so it flattens directly into one row per
#      contract per snapshot. `expiryData` describes which expiries exist for the
#      underlying (changes weekly/monthly, not per snapshot) so it's cached separately
#      as a small side table instead of being repeated on every options row.
#
# Both caches reuse the same partitioning idea as ParquetCacheManager
# (symbol=<percent-encoded>/year=/month=/day=) but at day granularity, and the same
# merge-then-rewrite-the-day-file approach so repeated snapshots the same day append
# instead of overwrite, keyed by (symbol, source, fetched_at).
# =====================================================================


def _fs_symbol(symbol: str) -> str:
    """Percent-encode a symbol for use as a Hive partition directory name.

    Symbols like 'NSE:SBIN-EQ' contain ':' which Windows forbids in directory
    names, so the raw symbol can't be used as a partition value directly.
    """
    return urllib.parse.quote(str(symbol), safe="")


class QuoteCacheManager:
    """Appends point-in-time quote snapshots, one Parquet file per symbol/day."""

    ENGINE = "pyarrow"

    def __init__(self, cache_dir: Path = Path(__file__).parent / "local_data" / "market_data_cache"):
        self.cache_dir = Path(cache_dir) / "quotes"

    def _partition_path(self, symbol: str, day: date_cls) -> Path:
        return (
            self.cache_dir
            / f"symbol={_fs_symbol(symbol)}"
            / f"year={day.year}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
        )

    def save(self, symbol: str, quote: dict, source: str, fetched_at: Optional[datetime] = None) -> None:
        """Persist one flat quote dict (e.g. Fyers' `v` sub-object) as a snapshot row."""
        fetched_at = fetched_at or datetime.now()
        row = {**quote, "symbol": symbol, "source": source, "fetched_at": fetched_at}
        df = pd.DataFrame([row])

        partition = self._partition_path(symbol, fetched_at.date())
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "part-0.parquet"

        if file_path.exists():
            existing = pd.read_parquet(file_path, engine=self.ENGINE)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["symbol", "source", "fetched_at"], keep="last")

        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), file_path)

    def save_fyers_response(self, response: dict, source: str = "fyers", fetched_at: Optional[datetime] = None) -> int:
        """Flatten and persist every symbol in a Fyers `quotes()` response. Returns rows saved."""
        entries = response.get("d", []) if isinstance(response, dict) else []
        count = 0
        for entry in entries:
            if entry.get("s") != "ok":
                continue
            symbol = entry.get("n") or entry.get("v", {}).get("symbol")
            if not symbol:
                continue
            self.save(symbol, entry.get("v", {}), source, fetched_at)
            count += 1
        return count

    def load(self, symbol: str, day: date_cls) -> pd.DataFrame:
        file_path = self._partition_path(symbol, day) / "part-0.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path, engine=self.ENGINE)


class OptionChainCacheManager:
    """Appends option-chain snapshots, one Parquet file per underlying/day.

    Flattens the optionsChain list (including the underlying row) into rows
    tagged with fetched_at, so intraday OI/price movement can be queried
    directly with pandas. `expiryData` is cached separately since it changes
    far less often than the chain itself.
    """

    ENGINE = "pyarrow"

    def __init__(self, cache_dir: Path = Path(__file__).parent / "local_data" / "market_data_cache"):
        self.cache_dir = Path(cache_dir) / "option_chain"

    def _partition_path(self, underlying: str, day: date_cls) -> Path:
        return (
            self.cache_dir
            / f"underlying={_fs_symbol(underlying)}"
            / f"year={day.year}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
        )

    def _expiry_path(self, underlying: str) -> Path:
        return self.cache_dir / f"underlying={_fs_symbol(underlying)}" / "expiries.parquet"

    def save_fyers_response(
        self,
        underlying: str,
        response: dict,
        source: str = "fyers",
        expiry_timestamp: str = "",
        fetched_at: Optional[datetime] = None,
    ) -> int:
        """Flatten and persist a Fyers `optionchain()` response. Returns rows saved."""
        data = response.get("data", {}) if isinstance(response, dict) else {}
        rows = data.get("optionsChain", [])
        if not rows:
            return 0

        fetched_at = fetched_at or datetime.now()
        df = pd.DataFrame(rows)
        df["underlying"] = underlying
        df["source"] = source
        df["expiry_timestamp"] = expiry_timestamp
        df["fetched_at"] = fetched_at

        partition = self._partition_path(underlying, fetched_at.date())
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "part-0.parquet"

        if file_path.exists():
            existing = pd.read_parquet(file_path, engine=self.ENGINE)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(
                subset=["symbol", "source", "expiry_timestamp", "fetched_at"], keep="last"
            )

        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), file_path)

        expiry_data = data.get("expiryData", [])
        if expiry_data:
            self._expiry_path(underlying).parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(pd.DataFrame(expiry_data), preserve_index=False),
                self._expiry_path(underlying),
            )

        return len(df)

    def load(self, underlying: str, day: date_cls) -> pd.DataFrame:
        file_path = self._partition_path(underlying, day) / "part-0.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path, engine=self.ENGINE)

    def load_expiries(self, underlying: str) -> pd.DataFrame:
        file_path = self._expiry_path(underlying)
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path, engine=self.ENGINE)

    def load_symbol_history(
        self, underlying: str, symbol: str, start_date: date_cls, end_date: date_cls
    ) -> pd.DataFrame:
        """Reconstruct one option contract's snapshot history from cached chain data.

        Serves a "history" request for a specific option symbol out of whatever
        option-chain snapshots were already fetched/cached for `underlying` in the
        given date range, without calling the broker again.
        """
        frames = []
        day = start_date
        while day <= end_date:
            day_df = self.load(underlying, day)
            if not day_df.empty:
                frames.append(day_df[day_df["symbol"] == symbol])
            day += timedelta(days=1)

        if not frames:
            return pd.DataFrame()

        return (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["symbol", "source", "expiry_timestamp", "fetched_at"], keep="last")
            .sort_values("fetched_at")
            .reset_index(drop=True)
        )
