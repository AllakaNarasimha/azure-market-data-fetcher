import os
import urllib.parse
from datetime import datetime, time, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


# =====================================================================
# CENTRALIZED CONSTANTS (Magic Strings Encapsulation)
# =====================================================================

class CacheConstants:
    MARKET_TIMEZONE = "Asia/Kolkata"
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)
    
    CACHE_DIR = "market_data_cache"
    REGISTRY_FILENAME = "market_off_registry.parquet"
    FORMAT = "parquet"
    ENGINE = "pyarrow"

    # Registry Status Reasons
    MARKET_CLOSED = "MARKET_CLOSED"
    WEEKEND = "WEEKEND"
    MARKET_HOLIDAY = "MARKET_HOLIDAY"
    API_EMPTY = "API_EMPTY"
    API_ERROR = "API_ERROR"
    DATA_GAP = "DATA_GAP"

    REGISTRY_COLUMNS = [
        "symbol",
        "interval",
        "start_time",
        "end_time",
        "reason",
    ]

    OHLCV_COLUMNS = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]


# =====================================================================
# 1. TIMESTAMP UTILITIES
# =====================================================================

class TimestampUtils:
    """Common timestamp conversion logic used by the cache."""

    @staticmethod
    def detect_unit(timestamp: int) -> str:
        """Detect whether timestamp is in seconds or milliseconds."""
        return "ms" if abs(int(timestamp)) > 2e10 else "s"

    @staticmethod
    def to_local(timestamp: int) -> datetime:
        """Convert epoch timestamp to Asia/Kolkata naive datetime (Candle OPEN time)."""
        unit = TimestampUtils.detect_unit(timestamp)
        return (
            pd.to_datetime(
                int(timestamp),
                unit=unit,
                utc=True,
            )
            .tz_convert(CacheConstants.MARKET_TIMEZONE)
            .tz_localize(None)
            .to_pydatetime()
        )

    @staticmethod
    def series_to_local(series: pd.Series) -> pd.Series:
        """Convert an epoch timestamp Series to Asia/Kolkata naive datetime."""
        numeric = pd.to_numeric(series, errors="coerce")

        if numeric.dropna().empty:
            return pd.Series(pd.NaT, index=series.index)

        unit = TimestampUtils.detect_unit(
            int(numeric.dropna().max())
        )

        return (
            pd.to_datetime(
                numeric,
                unit=unit,
                utc=True,
                errors="coerce",
            )
            .dt.tz_convert(CacheConstants.MARKET_TIMEZONE)
            .dt.tz_localize(None)
        )


# =====================================================================
# 2. CACHE REGISTRY
# =====================================================================

class CacheRegistry:
    """Persistent registry for known market-off periods and known data/API problems."""

    def __init__(self, cache_dir: str):
        self.registry_path = os.path.abspath(
            os.path.join(cache_dir, CacheConstants.REGISTRY_FILENAME)
        )
        os.makedirs(
            os.path.dirname(self.registry_path),
            exist_ok=True,
        )

    def load(self) -> pd.DataFrame:
        if (
            os.path.exists(self.registry_path)
            and os.path.getsize(self.registry_path) > 0
        ):
            try:
                return pd.read_parquet(
                    self.registry_path,
                    engine=CacheConstants.ENGINE,
                )
            except Exception as exc:
                print(f"⚠️ [REGISTRY] Failed to read registry: {exc}")

        return pd.DataFrame(columns=CacheConstants.REGISTRY_COLUMNS)

    def log_off(
        self,
        symbol: str,
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
        reason: str,
    ):
        """Persist a known market-off or data-problem interval."""
        try:
            df = self.load()
            new_row = pd.DataFrame(
                [{
                    "symbol": str(symbol),
                    "interval": str(interval),
                    "start_time": pd.to_datetime(start_dt),
                    "end_time": pd.to_datetime(end_dt),
                    "reason": str(reason),
                }]
            )

            df = pd.concat([df, new_row], ignore_index=True)
            df = df.drop_duplicates(
                subset=CacheConstants.REGISTRY_COLUMNS,
                keep="last",
            )

            df.to_parquet(
                self.registry_path,
                engine=CacheConstants.ENGINE,
                index=False,
            )

            print(f"📝 [REGISTRY] {symbol} | {interval} | {start_dt} → {end_dt} | {reason}")
        except Exception as exc:
            print(f"⚠️ [REGISTRY] Failed to save log: {exc}")

    def is_logged_as_off(
        self,
        symbol: str,
        interval: str,
        check_start: datetime,
        check_end: datetime,
    ) -> bool:
        """Returns True only when covered by a permanent MARKET_CLOSED/WEEKEND/MARKET_HOLIDAY entry."""
        df = self.load()
        if df.empty:
            return False

        valid_off_reasons = {
            CacheConstants.MARKET_CLOSED,
            CacheConstants.WEEKEND,
            CacheConstants.MARKET_HOLIDAY,
            CacheConstants.DATA_GAP,
            CacheConstants.API_EMPTY
        }

        matches = df[
            (df["symbol"] == str(symbol))
            & (df["interval"] == str(interval))
            & (df["reason"].isin(valid_off_reasons))
            & (df["start_time"] <= pd.to_datetime(check_start))
            & (df["end_time"] >= pd.to_datetime(check_end))
        ]
        return not matches.empty


# =====================================================================
# 3. MARKET CALENDAR / SESSION
# =====================================================================

class MarketSession:
    """Defines regular NSE-style trading sessions (Mon-Fri, 09:15-15:30)."""

    def __init__(
        self,
        market_open: time = CacheConstants.MARKET_OPEN,
        market_close: time = CacheConstants.MARKET_CLOSE,
    ):
        self.market_open = market_open
        self.market_close = market_close

    def is_trading_day(self, date_value) -> bool:
        return date_value.weekday() < 5

    def session_start(self, date_value) -> datetime:
        return datetime.combine(date_value, self.market_open)

    def session_end(self, date_value) -> datetime:
        return datetime.combine(date_value, self.market_close)


# =====================================================================
# 4. GAP DETECTOR (Extended for Intraday & Daily Intervals)
# =====================================================================

class CacheGapDetector:
    """Compares expected candle timestamps against cached timestamps to find missing ranges."""

    def __init__(self, market_session: Optional[MarketSession] = None):
        self.market_session = market_session or MarketSession()

    @staticmethod
    def get_interval_minutes(interval: str) -> int:
        clean_interval = str(interval).upper()
        if clean_interval.isdigit():
            return int(clean_interval)
        elif clean_interval in ("D", "1D"):
            return 1440
        raise ValueError(f"Unsupported interval: {interval}")

    def get_expected_timestamps(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: str,
    ) -> List[datetime]:
        clean_interval = str(interval).upper()
        expected = []
        current_date = start_date.date()
        final_date = end_date.date()

        if clean_interval in ("D", "1D"):
            while current_date <= final_date:
                if self.market_session.is_trading_day(current_date):
                    session_start = self.market_session.session_start(current_date)
                    if start_date <= session_start <= end_date:
                        expected.append(session_start)
                current_date += timedelta(days=1)
            return expected

        interval_mins = self.get_interval_minutes(interval)
        while current_date <= final_date:
            if self.market_session.is_trading_day(current_date):
                session_start = self.market_session.session_start(current_date)
                session_end = self.market_session.session_end(current_date)
                current = session_start

                while current < session_end:
                    if current >= start_date and current <= end_date:
                        expected.append(current)
                    current += timedelta(minutes=interval_mins)

            current_date += timedelta(days=1)

        return expected

    def get_missing_ranges(
        self,
        cached_records: List[Dict],
        start_date: datetime,
        end_date: datetime,
        interval: str,
        registry: CacheRegistry,
        symbol: str,
    ) -> List[Tuple[datetime, datetime]]:
        clean_interval = str(interval).upper()
        is_daily = clean_interval in ("D", "1D")
        interval_mins = self.get_interval_minutes(interval)
        cached_timestamps = set()

        for record in cached_records or []:
            timestamp = record.get("timestamp")
            if timestamp is None:
                continue
            try:
                dt = TimestampUtils.to_local(int(timestamp))
                cached_timestamps.add(dt.replace(second=0, microsecond=0))
            except (ValueError, TypeError):
                continue

        expected_timestamps = self.get_expected_timestamps(start_date, end_date, interval)
        if not expected_timestamps:
            return []

        missing_timestamps = []
        for timestamp in expected_timestamps:
            slot_start = timestamp
            slot_end = timestamp + timedelta(days=1) if is_daily else timestamp + timedelta(minutes=interval_mins)

            if timestamp in cached_timestamps:
                continue
            if registry.is_logged_as_off(symbol, interval, slot_start, slot_end):
                continue

            missing_timestamps.append(timestamp)

        if not missing_timestamps:
            return []

        ranges = []
        range_start = missing_timestamps[0]
        previous = missing_timestamps[0]

        for timestamp in missing_timestamps[1:]:
            expected_next = previous + timedelta(days=1) if is_daily else previous + timedelta(minutes=interval_mins)
            if timestamp != expected_next:
                range_end = previous + timedelta(days=1) if is_daily else previous + timedelta(minutes=interval_mins)
                ranges.append((range_start, range_end))
                range_start = timestamp
            previous = timestamp

        range_end = previous + timedelta(days=1) if is_daily else previous + timedelta(minutes=interval_mins)
        ranges.append((range_start, range_end))
        return ranges


# =====================================================================
# 5. PARQUET CACHE MANAGER (Extended for Intraday & Daily Support)
# =====================================================================

class ParquetCacheManager:
    """Main cache orchestrator supporting strict interval isolation and safe merge/upsert, with daily data support."""

    def __init__(
        self,
        cache_dir: str = CacheConstants.CACHE_DIR,
        market_session: Optional[MarketSession] = None,
    ):
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)

        self.registry = CacheRegistry(self.cache_dir)
        self.market_session = market_session or MarketSession()
        self.gap_detector = CacheGapDetector(self.market_session)

    @staticmethod
    def _fs_symbol(symbol: str) -> str:
        """Percent-encode symbol for use as a partition directory name.

        Symbols like 'NSE:SUZLON-EQ' contain ':' which Windows forbids in
        directory names (WinError 123), so the raw symbol can never be used
        as a Hive partition value directly.
        """
        return urllib.parse.quote(str(symbol), safe="")

    def save_candles(
        self,
        symbol: str,
        data: List[Dict],
        interval: str = "5",
        fetch_start: datetime = None,
        fetch_end: datetime = None,
    ) -> bool:
        if not data:
            if fetch_start and fetch_end:
                reason = self._classify_empty_period(fetch_start, fetch_end)
                self.registry.log_off(symbol, interval, fetch_start, fetch_end, reason)
            return False

        try:
            df = pd.DataFrame(data)
            if df.empty:
                return False

            self._validate_candle_dataframe(df)

            if fetch_start and fetch_end:
                missing_ranges = self.gap_detector.get_missing_ranges(
                    data, fetch_start, fetch_end, interval, self.registry, symbol
                )
                for gap_start, gap_end in missing_ranges:
                    self.registry.log_off(
                        symbol, interval, gap_start, gap_end, CacheConstants.DATA_GAP
                    )

            dt_series = TimestampUtils.series_to_local(df["timestamp"])
            df = df[dt_series.notna()].copy()
            dt_series = dt_series[dt_series.notna()]

            df["symbol"] = self._fs_symbol(symbol)
            df["interval"] = str(interval)
            df["_datetime"] = dt_series.values
            df["year"] = dt_series.dt.year.astype("int16")
            df["month"] = dt_series.dt.month.astype("int8")

            df = self._merge_with_existing_data(symbol, interval, df)

            self._write_partitions(symbol, interval, df)
            return True
        except Exception as exc:
            print(f"⚠️ [PARQUET] Failed to save {symbol}: {exc}")
            return False

    def clean_and_save_candles(
        self,
        symbol: str,
        data: List[Dict],
        interval: str = "5",
        fetch_start: datetime = None,
        fetch_end: datetime = None,
    ) -> bool:
        """Sanitize incoming candle data by coercing types and dropping invalid OHLCV rows,
        then persist the cleaned data via `save_candles`.

        Returns True when some data was saved, False otherwise.
        """
        if not data:
            return self.save_candles(symbol, data, interval, fetch_start, fetch_end)

        try:
            df = pd.DataFrame(data)
            if df.empty:
                return False

            # Coerce numeric types
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df.get(col), errors="coerce")
            df["timestamp"] = pd.to_numeric(df.get("timestamp"), errors="coerce")

            # Drop rows missing essential OHLCV/timestamp
            df.dropna(subset=CacheConstants.OHLCV_COLUMNS, inplace=True)

            # Identify invalid OHLCV rows
            invalid_ohlc = (
                (df["high"] < df["open"]) 
                | (df["high"] < df["close"]) 
                | (df["low"] > df["open"]) 
                | (df["low"] > df["close"]) 
                | (df["high"] < df["low"]) 
                | (df["volume"] < 0)
            )

            invalid_count = int(invalid_ohlc.sum()) if not invalid_ohlc.empty else 0
            if invalid_count > 0:
                print(f"⚠️ [PARQUET] Dropping {invalid_count} invalid OHLCV rows for {symbol}")
                df = df[~invalid_ohlc]

            cleaned = df.to_dict("records")
            return self.save_candles(symbol, cleaned, interval, fetch_start, fetch_end)
        except Exception as exc:
            print(f"⚠️ [PARQUET] clean_and_save_candles failed for {symbol}: {exc}")
            return False
            
    def load_candles(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "5",
    ) -> List[Dict]:
        if not os.path.exists(self.cache_dir) or not os.listdir(self.cache_dir):
            return []

        try:
            clean_interval = str(interval).upper()
            df = self._query_dataset(symbol, interval, start_date, end_date)

            # Fallback resampling for intraday or daily intervals if missing data exists and 1m is available
            if clean_interval not in ("1", "D", "1D"):
                expected = self.gap_detector.get_expected_timestamps(
                    start_date, end_date, interval
                )
                cached_timestamps = self._get_local_timestamps(df)
                missing = [ts for ts in expected if ts not in cached_timestamps]

                if df.empty or missing:
                    df_1m = self._query_dataset(symbol, "1", start_date, end_date)
                    if not df_1m.empty:
                        df_from_1m = self._resample_dataframe(df_1m, interval)
                        if not df_from_1m.empty:
                            if not df.empty:
                                df = pd.concat([df, df_from_1m], ignore_index=True)
                                df = (
                                    df.drop_duplicates(subset=["timestamp"], keep="last")
                                    .sort_values("timestamp")
                                )
                            else:
                                df = df_from_1m

                            self.save_candles(
                                symbol,
                                df.to_dict("records"),
                                interval=interval,
                                fetch_start=start_date,
                                fetch_end=end_date,
                            )

            elif clean_interval in ("D", "1D") and df.empty:
                df_1m = self._query_dataset(symbol, "1", start_date, end_date)
                if not df_1m.empty:
                    df_from_1m = self._resample_dataframe(df_1m, interval)
                    if not df_from_1m.empty:
                        df = df_from_1m
                        self.save_candles(
                            symbol,
                            df.to_dict("records"),
                            interval=interval,
                            fetch_start=start_date,
                            fetch_end=end_date,
                        )

            if df.empty:
                return []

            df = df.copy()
            df["_datetime"] = TimestampUtils.series_to_local(df["timestamp"])
            df = df[(df["_datetime"] >= start_date) & (df["_datetime"] <= end_date)]

            df = (
                df.sort_values("_datetime")
                .drop_duplicates(subset=["timestamp"], keep="last")
            )

            metadata_columns = ["symbol", "interval", "year", "month", "_datetime"]
            df = df.drop(
                columns=[col for col in metadata_columns if col in df.columns],
                errors="ignore",
            )

            return df.to_dict("records")
        except Exception as exc:
            print(f"⚠️ [PARQUET] Failed to load {symbol}: {exc}")
            return []

    def get_missing_date_ranges(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "5",
    ) -> List[Tuple[datetime, datetime]]:
        cached_records = self.load_candles(symbol, start_date, end_date, interval)
        return self.gap_detector.get_missing_ranges(
            cached_records, start_date, end_date, interval, self.registry, symbol
        )

    def _query_dataset(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Read only the specific year/month partition directories in range directly,
        instead of scanning the whole `cache_dir` with `ds.dataset(...)`.

        A whole-tree scan re-infers partition column types (e.g. int vs string for
        numeric-looking `interval`/`year`/`month` folder names) from every fragment
        under `cache_dir` — including the unrelated registry parquet file and any
        legacy-format partitions — which breaks with 'Unable to merge: Field ... has
        incompatible types' or 'No match for FieldRef' whenever that inference disagrees
        across fragments. Reading known partition paths directly sidesteps all of that.
        """
        frames = []
        for year in range(start_date.year, end_date.year + 1):
            month_start = start_date.month if year == start_date.year else 1
            month_end = end_date.month if year == end_date.year else 12
            for month in range(month_start, month_end + 1):
                part_df = self._load_partition(symbol, interval, year, month)
                if not part_df.empty:
                    frames.append(part_df)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def _resample_dataframe(
        self,
        df: pd.DataFrame,
        target_interval: str,
    ) -> pd.DataFrame:
        if df.empty or "timestamp" not in df.columns:
            return pd.DataFrame()

        clean_target = str(target_interval).upper()
        df = df.copy()
        df["datetime"] = TimestampUtils.series_to_local(df["timestamp"])
        df = (
            df.dropna(subset=["datetime"])
            .sort_values("datetime")
            .set_index("datetime")
        )

        df = df[
            (df.index.time >= self.market_session.market_open)
            & (df.index.time <= self.market_session.market_close)
        ]

        if df.empty:
            return pd.DataFrame()

        # Daily Resampling Support
        if clean_target in ("D", "1D"):
            result = (
                df.groupby(df.index.date)
                .agg({
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                })
                .reset_index()
                .rename(columns={"index": "datetime"})
            )

            result["datetime"] = pd.to_datetime(result["datetime"]).apply(
                lambda d: datetime.combine(d, self.market_session.market_open)
            )
            result["timestamp"] = result["datetime"].apply(
                lambda dt: int(
                    pd.Timestamp(dt)
                    .tz_localize(CacheConstants.MARKET_TIMEZONE)
                    .tz_convert("UTC")
                    .timestamp()
                )
            )
            return result[CacheConstants.OHLCV_COLUMNS]

        if not str(target_interval).isdigit():
            raise ValueError(f"Unsupported target interval: {target_interval}")

        minutes = int(target_interval)
        result = (
            df.groupby(df.index.date)
            .apply(lambda day: self._resample_session(day, minutes), include_groups=False)
            .reset_index(drop=True)
        )

        return result if not result.empty else pd.DataFrame()

    def _resample_session(self, day_df: pd.DataFrame, minutes: int) -> pd.DataFrame:
        day_df = day_df.sort_index()
        result = day_df.resample(
            f"{minutes}min", origin="start_day", offset="9h15min"
        ).agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})

        candle_counts = day_df["close"].resample(
            f"{minutes}min", origin="start_day", offset="9h15min"
        ).count()

        result["_source_count"] = candle_counts
        result = result[result["_source_count"] == minutes]
        result = result.drop(columns=["_source_count"]).dropna(subset=["open", "high", "low", "close"])

        if result.empty:
            return pd.DataFrame()

        result = result.reset_index()
        result["timestamp"] = result["datetime"].apply(
            lambda dt: int(
                pd.Timestamp(dt)
                .tz_localize(CacheConstants.MARKET_TIMEZONE)
                .tz_convert("UTC")
                .timestamp()
            )
        )

        return result[CacheConstants.OHLCV_COLUMNS]

    @staticmethod
    def _get_local_timestamps(df: pd.DataFrame) -> set:
        if df is None or df.empty or "timestamp" not in df.columns:
            return set()
        timestamps = TimestampUtils.series_to_local(df["timestamp"])
        return {ts.replace(second=0, microsecond=0) for ts in timestamps.dropna()}

    @staticmethod
    def _validate_candle_dataframe(df: pd.DataFrame):
        missing_columns = [col for col in CacheConstants.OHLCV_COLUMNS if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing OHLCV columns: {missing_columns}")

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")

        df.dropna(subset=CacheConstants.OHLCV_COLUMNS, inplace=True)

        invalid_ohlc = (
            (df["high"] < df["open"])
            | (df["high"] < df["close"])
            | (df["low"] > df["open"])
            | (df["low"] > df["close"])
            | (df["high"] < df["low"])
            | (df["volume"] < 0)
        )

        if invalid_ohlc.any():
            raise ValueError(f"Invalid OHLCV rows found: {int(invalid_ohlc.sum())}")

    @staticmethod
    def _classify_empty_period(start_date: datetime, end_date: datetime) -> str:
        current = start_date.date()
        while current <= end_date.date():
            if current.weekday() < 5:
                return CacheConstants.API_EMPTY
            current += timedelta(days=1)
        return CacheConstants.WEEKEND

    def _merge_with_existing_data(
        self,
        symbol: str,
        interval: str,
        new_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if new_df.empty:
            return new_df

        result = new_df.copy()
        partitions = result[["year", "month"]].drop_duplicates().to_dict("records")

        for partition in partitions:
            year, month = partition["year"], partition["month"]
            existing = self._load_partition(symbol, interval, year, month)

            if existing.empty:
                continue

            existing["_datetime"] = TimestampUtils.series_to_local(existing["timestamp"])
            existing["year"] = existing["_datetime"].dt.year.astype("int16")
            existing["month"] = existing["_datetime"].dt.month.astype("int8")
            existing["symbol"] = self._fs_symbol(symbol)
            existing["interval"] = str(interval)

            result = pd.concat([existing, result], ignore_index=True)

        return (
            result.drop_duplicates(subset=["timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    def _partition_path(self, symbol: str, interval: str, year: int, month: int) -> str:
        return os.path.join(
            self.cache_dir,
            f"symbol={self._fs_symbol(symbol)}",
            f"interval={interval}",
            f"year={year}",
            f"month={month}",
        )

    def _write_partitions(self, symbol: str, interval: str, df: pd.DataFrame) -> None:
        """Write each (year, month) group directly to its partition path.

        Bypasses `ds.write_dataset`'s partitioning/escaping (which double-encodes an
        already percent-encoded symbol, e.g. 'NSE%3A...' -> 'NSE%253A...'), so the
        directory this writes to always matches exactly what `_load_partition` reads.
        """
        for (year, month), group in df.groupby(["year", "month"]):
            partition_path = self._partition_path(symbol, interval, int(year), int(month))
            os.makedirs(partition_path, exist_ok=True)
            write_df = group.drop(columns=["symbol", "interval", "year", "month"], errors="ignore")
            table = pa.Table.from_pandas(write_df, preserve_index=False)
            pq.write_table(table, os.path.join(partition_path, "part-0.parquet"))

    def _load_partition(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        partition_path = self._partition_path(symbol, interval, year, month)

        if not os.path.exists(partition_path):
            return pd.DataFrame()

        try:
            return pd.read_parquet(partition_path, engine=CacheConstants.ENGINE)
        except Exception as exc:
            print(f"⚠️ [PARQUET] Failed to read partition {partition_path}: {exc}")
            return pd.DataFrame()
