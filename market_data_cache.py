from __future__ import annotations

import urllib.parse
from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from parquet_cache_manager import BlobSync, _local_cache_root

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

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = Path(cache_dir or _local_cache_root("market_data_cache")) / "quotes"
        self._blob_sync = BlobSync()

    def _partition_path(self, symbol: str, day: date_cls) -> Path:
        return (
            self.cache_dir
            / f"symbol={_fs_symbol(symbol)}"
            / f"year={day.year}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
        )

    def _blob_name(self, symbol: str, day: date_cls) -> str:
        return f"quotes/symbol={_fs_symbol(symbol)}/year={day.year}/month={day.month:02d}/day={day.day:02d}/part-0.parquet"

    def save(self, symbol: str, quote: dict, source: str, fetched_at: Optional[datetime] = None) -> None:
        """Persist one flat quote dict (e.g. Fyers' `v` sub-object) as a snapshot row."""
        fetched_at = fetched_at or datetime.now()
        row = {**quote, "symbol": symbol, "source": source, "fetched_at": fetched_at}
        df = pd.DataFrame([row])

        partition = self._partition_path(symbol, fetched_at.date())
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "part-0.parquet"
        blob_name = self._blob_name(symbol, fetched_at.date())
        self._blob_sync.download_if_missing(str(file_path), blob_name)

        if file_path.exists():
            existing = pd.read_parquet(file_path, engine=self.ENGINE)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["symbol", "source", "fetched_at"], keep="last")

        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), file_path)
        self._blob_sync.upload(str(file_path), blob_name)

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
        self._blob_sync.download_if_missing(str(file_path), self._blob_name(symbol, day))
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

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = Path(cache_dir or _local_cache_root("market_data_cache")) / "option_chain"
        self._blob_sync = BlobSync()

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

    def _blob_name(self, underlying: str, day: date_cls) -> str:
        return f"option_chain/underlying={_fs_symbol(underlying)}/year={day.year}/month={day.month:02d}/day={day.day:02d}/part-0.parquet"

    def _expiry_blob_name(self, underlying: str) -> str:
        return f"option_chain/underlying={_fs_symbol(underlying)}/expiries.parquet"

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
        blob_name = self._blob_name(underlying, fetched_at.date())
        self._blob_sync.download_if_missing(str(file_path), blob_name)

        if file_path.exists():
            existing = pd.read_parquet(file_path, engine=self.ENGINE)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(
                subset=["symbol", "source", "expiry_timestamp", "fetched_at"], keep="last"
            )

        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), file_path)
        self._blob_sync.upload(str(file_path), blob_name)

        expiry_data = data.get("expiryData", [])
        if expiry_data:
            expiry_path = self._expiry_path(underlying)
            expiry_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(pd.DataFrame(expiry_data), preserve_index=False),
                expiry_path,
            )
            self._blob_sync.upload(str(expiry_path), self._expiry_blob_name(underlying))

        return len(df)

    def save_fyers_responses_batch(
        self,
        underlying: str,
        responses: list[tuple[dict, str]],
        source: str = "fyers",
        fetched_at: Optional[datetime] = None,
    ) -> int:
        """Flatten and persist multiple `optionchain()` responses (one per expiry) for the
        same underlying/day in a single read-modify-write-upload cycle, instead of one per
        response - avoids repeatedly re-reading/re-writing/re-uploading the same growing
        day-partition file once per weekly/monthly expiry.

        `responses` is a list of (response, expiry_timestamp) pairs.
        """
        fetched_at = fetched_at or datetime.now()
        frames = []
        expiry_data = None
        for response, expiry_timestamp in responses:
            data = response.get("data", {}) if isinstance(response, dict) else {}
            rows = data.get("optionsChain", [])
            if rows:
                df = pd.DataFrame(rows)
                df["underlying"] = underlying
                df["source"] = source
                df["expiry_timestamp"] = expiry_timestamp
                df["fetched_at"] = fetched_at
                frames.append(df)
            if data.get("expiryData"):
                expiry_data = data["expiryData"]

        saved_rows = 0
        if frames:
            df = pd.concat(frames, ignore_index=True)

            partition = self._partition_path(underlying, fetched_at.date())
            partition.mkdir(parents=True, exist_ok=True)
            file_path = partition / "part-0.parquet"
            blob_name = self._blob_name(underlying, fetched_at.date())
            self._blob_sync.download_if_missing(str(file_path), blob_name)

            if file_path.exists():
                existing = pd.read_parquet(file_path, engine=self.ENGINE)
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(
                    subset=["symbol", "source", "expiry_timestamp", "fetched_at"], keep="last"
                )

            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), file_path)
            self._blob_sync.upload(str(file_path), blob_name)
            saved_rows = len(df)

        if expiry_data:
            expiry_path = self._expiry_path(underlying)
            expiry_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(pd.DataFrame(expiry_data), preserve_index=False),
                expiry_path,
            )
            self._blob_sync.upload(str(expiry_path), self._expiry_blob_name(underlying))

        return saved_rows

    def load(self, underlying: str, day: date_cls) -> pd.DataFrame:
        file_path = self._partition_path(underlying, day) / "part-0.parquet"
        self._blob_sync.download_if_missing(str(file_path), self._blob_name(underlying, day))
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path, engine=self.ENGINE)

    def load_expiries(self, underlying: str) -> pd.DataFrame:
        file_path = self._expiry_path(underlying)
        self._blob_sync.download_if_missing(str(file_path), self._expiry_blob_name(underlying))
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
