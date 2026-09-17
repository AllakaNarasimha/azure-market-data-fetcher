from __future__ import annotations

import io
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient

from broker_authenticate import BrokerAuth

logger = logging.getLogger(__name__)


class MasterFileCache:
    """Downloads reference/master files once per day and reuses them.

    Locally these are cached under local_data/; in Azure Functions (read-only
    filesystem when deployed) they are cached in Blob Storage instead, using
    the same is_local pattern as TokenCache.
    """

    CONTAINER = "broker-master"

    def __init__(self, local_dir: Path):
        self.is_local = os.getenv("WEBSITE_INSTANCE_ID") is None
        self.local_dir = local_dir
        self._container_client = None
        if not self.is_local:
            conn_str = os.getenv("MARKET_STORAGE_CONNECTION")
            if conn_str:
                blob_service = BlobServiceClient.from_connection_string(conn_str)
                self._container_client = blob_service.get_container_client(self.CONTAINER)
                try:
                    self._container_client.create_container()
                except Exception:
                    pass

    def get(self, filename: str, download_url: str) -> bytes:
        if self.is_local:
            self.local_dir.mkdir(parents=True, exist_ok=True)
            path = self.local_dir / filename
            if not path.exists():
                resp = requests.get(download_url, timeout=30)
                resp.raise_for_status()
                path.write_bytes(resp.content)
            return path.read_bytes()

        if not self._container_client:
            logger.warning("MARKET_STORAGE_CONNECTION not set; fetching %s without caching", filename)
            resp = requests.get(download_url, timeout=30)
            resp.raise_for_status()
            return resp.content

        blob = self._container_client.get_blob_client(filename)
        if blob.exists():
            return blob.download_blob().readall()
        resp = requests.get(download_url, timeout=30)
        resp.raise_for_status()
        blob.upload_blob(resp.content, overwrite=True)
        return resp.content


class DhanSecurityMaster:
    """Resolves Dhan's numeric security_id from the public Dhan scrip master CSV.

    Dhan has no deterministic symbol -> security_id formula, so the daily CSV
    must be downloaded and looked up; Fyers symbols, by contrast, follow a
    fixed 'EXCHANGE:SYMBOL-EQ' convention and need no such lookup.
    """

    BASE_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    LOCAL_DIR = Path(__file__).parent / "local_data" / "dhan_master"
    ALLOWED_FILTER_COLUMNS = {
        "SEM_EXM_EXCH_ID",
        "SEM_INSTRUMENT_NAME",
        "SEM_TRADING_SYMBOL",
        "SM_SYMBOL_NAME",
        "SEM_STRIKE_PRICE",
        "SEM_OPTION_TYPE",
        "SEM_EXPIRY_DATE",
    }

    def __init__(self):
        self._cache: dict[str, pd.DataFrame] = {}

    def _load(self, date: str) -> pd.DataFrame:
        if date not in self._cache:
            content = MasterFileCache(self.LOCAL_DIR).get(f"master_{date}.csv", self.BASE_URL)
            self._cache[date] = pd.read_csv(io.BytesIO(content))
        return self._cache[date]

    def get_security_id(
        self, trading_symbol: str, exchange: str = "NSE", instrument_name: str = "EQUITY"
    ) -> Optional[int]:
        df = self._load(time.strftime("%Y-%m-%d"))
        filters = {
            "SEM_EXM_EXCH_ID": exchange,
            "SEM_INSTRUMENT_NAME": instrument_name,
            "SEM_TRADING_SYMBOL": trading_symbol,
        }
        matched = df
        for col, val in filters.items():
            if col not in self.ALLOWED_FILTER_COLUMNS:
                raise ValueError(f"Invalid filter column: {col}")
            matched = matched[matched[col].astype(str).str.upper() == str(val).upper()]
        if matched.empty:
            return None
        return int(matched.iloc[0]["SEM_SMST_SECURITY_ID"])

    def get_option_security_id(
        self,
        underlying: str,
        strike: float,
        option_type: str,
        exchange: str = "NSE",
        instrument_name: str = "OPTIDX",
        expiry_date: Optional[str] = None,
    ) -> Optional[int]:
        df = self._load(time.strftime("%Y-%m-%d"))
        matched = df[
            (df["SEM_EXM_EXCH_ID"].astype(str).str.upper() == exchange.upper())
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == instrument_name.upper())
            & (df["SM_SYMBOL_NAME"].astype(str).str.upper() == underlying.upper())
            & (df["SEM_STRIKE_PRICE"].astype(float) == float(strike))
            & (df["SEM_OPTION_TYPE"].astype(str).str.upper() == option_type.upper())
        ]
        if expiry_date:
            matched = matched[matched["SEM_EXPIRY_DATE"].astype(str).str.startswith(expiry_date)]
        if matched.empty:
            return None
        return int(matched.iloc[0]["SEM_SMST_SECURITY_ID"])


class FyersSymbolMaster:
    """Validates Fyers tickers and resolves option symbols from Fyers' public CSVs.

    Fyers REST calls take the ticker string directly (no numeric id), but the
    ticker still needs checking against the daily master file: equity tickers
    can be delisted/renamed, and option tickers embed strike/expiry so they
    can't be built from the underlying name alone.
    """

    CM_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
    FO_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
    LOCAL_DIR = Path(__file__).parent / "local_data" / "fyers_master"
    COL_SYMBOL = 9
    COL_UNDERLYING = 13
    COL_STRIKE = 15
    COL_OPTION_TYPE = 16
    COL_EXPIRY = 8

    def __init__(self):
        self._cache: dict[str, pd.DataFrame] = {}

    def _load(self, url: str, name: str, date: str) -> pd.DataFrame:
        key = f"{name}_{date}"
        if key not in self._cache:
            content = MasterFileCache(self.LOCAL_DIR).get(f"{key}.csv", url)
            self._cache[key] = pd.read_csv(io.BytesIO(content), header=None)
        return self._cache[key]

    def validate_equity_symbol(self, symbol: str) -> bool:
        df = self._load(self.CM_URL, "nse_cm", time.strftime("%Y-%m-%d"))
        return bool((df[self.COL_SYMBOL].astype(str).str.upper() == symbol.upper()).any())

    def get_option_symbol(
        self, underlying: str, strike: float, option_type: str, expiry_date: Optional[str] = None
    ) -> Optional[str]:
        df = self._load(self.FO_URL, "nse_fo", time.strftime("%Y-%m-%d"))
        matched = df[
            (df[self.COL_UNDERLYING].astype(str).str.upper() == underlying.upper())
            & (df[self.COL_STRIKE].astype(float) == float(strike))
            & (df[self.COL_OPTION_TYPE].astype(str).str.upper() == option_type.upper())
        ]
        if expiry_date:
            matched = matched[matched[self.COL_EXPIRY].astype(str) == expiry_date]
        if matched.empty:
            return None
        return str(matched.iloc[0][self.COL_SYMBOL])


@dataclass
class Instrument:
    """Broker-agnostic identifier: Dhan reads security_id, Fyers reads symbol."""

    symbol: str
    security_id: Optional[int] = None
    exchange_segment: str = "NSE_EQ"


class InstrumentResolver:
    """Single entry point for building broker-ready Instrument objects."""

    INDEX_SYMBOLS = {
        "NIFTY": "NSE:NIFTY50-INDEX",
        "NIFTY50": "NSE:NIFTY50-INDEX",
        "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
        "NIFTYBANK": "NSE:NIFTYBANK-INDEX",
    }

    @staticmethod
    def resolve(
        symbol: str, exchange: str = "NSE", instrument_name: str = "EQUITY", exchange_segment: str = "NSE_EQ"
    ) -> Instrument:
        """Build an Instrument with both broker identifiers filled in from a plain symbol."""
        index_symbol = InstrumentResolver.INDEX_SYMBOLS.get(symbol.upper())
        if index_symbol:
            security_id = None
            try:
                security_id = DhanSecurityMaster().get_security_id(symbol, exchange, "INDEX")
            except Exception:
                logger.exception("Failed to resolve Dhan security_id for index %s", symbol)
            return Instrument(symbol=index_symbol, security_id=security_id, exchange_segment="IDX_I")

        fyers_symbol = symbol if ":" in symbol else f"{exchange}:{symbol}-EQ"
        security_id = None
        try:
            security_id = DhanSecurityMaster().get_security_id(symbol, exchange, instrument_name)
        except Exception:
            logger.exception("Failed to resolve Dhan security_id for %s", symbol)
        try:
            if not FyersSymbolMaster().validate_equity_symbol(fyers_symbol):
                logger.warning("Fyers symbol %s not found in NSE_CM master", fyers_symbol)
        except Exception:
            logger.exception("Failed to validate Fyers symbol %s", fyers_symbol)
        return Instrument(symbol=fyers_symbol, security_id=security_id, exchange_segment=exchange_segment)

    @staticmethod
    def resolve_option(
        underlying: str,
        strike: float,
        option_type: str,
        exchange: str = "NSE",
        instrument_name: str = "OPTIDX",
        expiry_date: Optional[str] = None,
    ) -> Instrument:
        """Build an option Instrument for both brokers from underlying/strike/option_type."""
        security_id = None
        try:
            security_id = DhanSecurityMaster().get_option_security_id(
                underlying, strike, option_type, exchange, instrument_name, expiry_date
            )
        except Exception:
            logger.exception(
                "Failed to resolve Dhan option security_id for %s %s %s", underlying, strike, option_type
            )

        fyers_symbol = None
        try:
            fyers_symbol = FyersSymbolMaster().get_option_symbol(underlying, strike, option_type, expiry_date)
        except Exception:
            logger.exception("Failed to resolve Fyers option symbol for %s %s %s", underlying, strike, option_type)
        if not fyers_symbol:
            raise RuntimeError(f"Could not resolve Fyers option symbol for {underlying} {strike} {option_type}")

        return Instrument(symbol=fyers_symbol, security_id=security_id, exchange_segment="NSE_FNO")


class ExpiryResolver:
    """Resolves weekly_expiry_count/monthly_expiry_count selectors to an expiry timestamp.

    Shared by all IDataManager implementations so brokers don't each reimplement
    the same selection convention:
      - weekly_expiry_count: None/0 = current week, 1 = next week, 2 = week after, ...
      - monthly_expiry_count: None (default) = weekly-only mode, ignored;
        0 = current month, 1 = next month, ...
    When monthly_expiry_count is given it takes precedence over weekly_expiry_count.
    """

    @staticmethod
    def split(expiries: list[dict]) -> tuple[list[dict], list[dict]]:
        """Split raw expiry entries into (weekly, monthly) lists, sorted ascending.

        De-duplicates by expiry timestamp: when a weekly expiry coincides with the
        month's expiry (last week of the month), it is kept only in the monthly
        list - one date is enough, no need to fetch/list it twice.
        """
        by_ts: dict[str, dict] = {}
        for entry in expiries:
            by_ts.setdefault(entry["expiry"], entry)

        monthly_ts = {ts for ts, e in by_ts.items() if str(e.get("expiry_flag", "")).upper() == "M"}
        weekly = sorted(
            (e for ts, e in by_ts.items() if str(e.get("expiry_flag", "")).upper() == "W" and ts not in monthly_ts),
            key=lambda e: int(e["expiry"]),
        )
        monthly = sorted((e for ts in monthly_ts for e in [by_ts[ts]]), key=lambda e: int(e["expiry"]))
        return weekly, monthly

    @staticmethod
    def resolve(
        expiries: list[dict],
        weekly_expiry_count: Optional[int] = None,
        monthly_expiry_count: Optional[int] = None,
    ) -> str:
        """Return the epoch timestamp string for the requested week/month selector."""
        weekly, monthly = ExpiryResolver.split(expiries)

        if monthly_expiry_count is not None:
            if monthly_expiry_count >= len(monthly):
                raise IndexError(
                    f"monthly_expiry_count {monthly_expiry_count} out of range; "
                    f"only {len(monthly)} monthly expiries available"
                )
            return monthly[monthly_expiry_count]["expiry"]

        count = weekly_expiry_count or 0
        if count >= len(weekly):
            raise IndexError(
                f"weekly_expiry_count {count} out of range; only {len(weekly)} weekly expiries available"
            )
        return weekly[count]["expiry"]

    @staticmethod
    def classify_dates(expiry_dates: list[str]) -> list[dict]:
        """Best-effort weekly/monthly classification for brokers (e.g. Dhan) whose
        expiry list has no explicit flag: an expiry is 'monthly' when it's the
        last expiry falling in its calendar month among the given dates.
        """
        parsed = sorted({datetime.strptime(d, "%Y-%m-%d") for d in expiry_dates})
        result = []
        for i, dt in enumerate(parsed):
            is_last_of_month = (i == len(parsed) - 1) or (parsed[i + 1].month != dt.month)
            result.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "expiry": dt.strftime("%Y-%m-%d"),
                    "expiry_flag": "M" if is_last_of_month else "W",
                }
            )
        return result


class IDataManager:
    def get_price(self, instrument: Instrument) -> dict:
        raise NotImplementedError()

    def get_quotes(self, instruments: list[Instrument]) -> list[dict]:
        raise NotImplementedError()

    def get_historical_data(
        self, instrument: Instrument, period: str, otype: str, start: int, end: int
    ) -> list[tuple]:
        raise NotImplementedError()

    def get_option_chain(
        self,
        instrument: Instrument,
        strikecount: int = 1,
        weekly_expiry_count: Optional[int] = None,
        monthly_expiry_count: Optional[int] = None,
    ) -> dict:
        raise NotImplementedError()


class DhanDataManager(IDataManager):
    def __init__(self, auth: Optional[BrokerAuth] = None):
        self._auth = auth or BrokerAuth()
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = self._auth.get_dhan_client()
        return self._client

    @staticmethod
    def _security_id(instrument: Instrument) -> int:
        if instrument.security_id is None:
            raise ValueError("Dhan requires Instrument.security_id")
        return instrument.security_id

    def get_price(self, instrument: Instrument) -> dict:
        sec_id = self._security_id(instrument)
        return self._get_client().quote_data({instrument.exchange_segment: [sec_id]})

    def get_quotes(self, instruments: list[Instrument]) -> list[dict]:
        by_segment: dict[str, list[int]] = {}
        for inst in instruments:
            by_segment.setdefault(inst.exchange_segment, []).append(self._security_id(inst))
        return self._get_client().quote_data(by_segment)

    def get_historical_data(
        self, instrument: Instrument, period: str, otype: str, start: int, end: int
    ) -> list[tuple]:
        client = self._get_client()
        sec_id = self._security_id(instrument)
        if period.upper() in ("1D", "D", "DAILY"):
            return client.historical_daily_data(
                security_id=str(sec_id),
                exchange_segment=instrument.exchange_segment,
                instrument_type=otype,
                expiry_code=0,
                from_date=str(start),
                to_date=str(end),
            )
        return client.intraday_minute_data(
            security_id=str(sec_id),
            exchange_segment=instrument.exchange_segment,
            instrument_type=otype,
            interval=period,
            from_date=str(start),
            to_date=str(end),
        )

    def get_option_chain(
        self,
        instrument: Instrument,
        strikecount: int = 1,
        weekly_expiry_count: Optional[int] = None,
        monthly_expiry_count: Optional[int] = None,
    ) -> dict:
        sec_id = self._security_id(instrument)
        expiry = ""
        if weekly_expiry_count is not None or monthly_expiry_count is not None:
            expiries = ExpiryResolver.classify_dates(self.get_expiry_dates(instrument))
            expiry = ExpiryResolver.resolve(expiries, weekly_expiry_count, monthly_expiry_count)
        return self._get_client().option_chain(
            under_security_id=sec_id, under_exchange_segment="NSE_FNO", expiry=expiry
        )

    def get_expiry_dates(self, instrument: Instrument) -> list[str]:
        """Raw expiry date strings for the underlying (Dhan has no weekly/monthly flag)."""
        sec_id = self._security_id(instrument)
        resp = self._get_client().expiry_list(under_security_id=sec_id, under_exchange_segment="NSE_FNO")
        data = resp.get("data", resp) if isinstance(resp, dict) else resp
        return data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []


class FyersDataManager(IDataManager):
    def __init__(self, auth: Optional[BrokerAuth] = None):
        self._auth = auth or BrokerAuth()
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = self._auth.get_fyers_client()
        return self._client

    def get_price(self, instrument: Instrument) -> dict:
        return self._get_client().quotes({"symbols": instrument.symbol})

    def get_quotes(self, instruments: list[Instrument]) -> list[dict]:
        symbols = ",".join(inst.symbol for inst in instruments)
        return self._get_client().quotes({"symbols": symbols})

    def get_historical_data(
        self, instrument: Instrument, period: str, otype: str, start: int, end: int
    ) -> list[tuple]:
        return self._get_client().history(
            {
                "symbol": instrument.symbol,
                "resolution": period,
                "date_format": "1",
                "range_from": str(start),
                "range_to": str(end),
                "cont_flag": "1",
            }
        )

    def get_option_chain(
        self,
        instrument: Instrument,
        strikecount: int = 1,
        weekly_expiry_count: Optional[int] = None,
        monthly_expiry_count: Optional[int] = None,
    ) -> dict:
        timestamp = ""
        if weekly_expiry_count is not None or monthly_expiry_count is not None:
            timestamp = ExpiryResolver.resolve(self.get_expiries(instrument), weekly_expiry_count, monthly_expiry_count)
        return self._get_client().optionchain(
            {"symbol": instrument.symbol, "strikecount": strikecount, "timestamp": timestamp}
        )

    def get_expiries(self, instrument: Instrument) -> list[dict]:
        """Available expiries for the underlying: [{date, expiry (epoch str), expiry_flag}, ...], nearest first."""
        chain = self._get_client().optionchain({"symbol": instrument.symbol, "strikecount": 1, "timestamp": ""})
        return chain.get("data", {}).get("expiryData", [])


class HistoryNormalizer:
    """Converts each broker's get_historical_data() response into the
    list[dict] shape (timestamp, open, high, low, close, volume) that
    ParquetCacheManager.save_candles()/clean_and_save_candles() expect.
    """

    @staticmethod
    def to_candles(source: str, response: dict) -> list[dict]:
        if source == "fyers":
            return [
                {"timestamp": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]}
                for c in (response or {}).get("candles", [])
            ]
        if source == "dhan":
            data = response.get("data", {}) if isinstance(response, dict) else {}
            if not isinstance(data, dict):
                return []
            timestamps = data.get("timestamp") or data.get("start_Time") or []
            opens, highs, lows, closes, volumes = (
                data.get("open", []),
                data.get("high", []),
                data.get("low", []),
                data.get("close", []),
                data.get("volume", []),
            )
            return [
                {
                    "timestamp": timestamps[i],
                    "open": opens[i],
                    "high": highs[i],
                    "low": lows[i],
                    "close": closes[i],
                    "volume": volumes[i],
                }
                for i in range(len(timestamps))
            ]
        raise ValueError(f"Unknown history source: {source}")


class PreferredBrokers:
    """Resolves which broker(s) to call from the PREFER_BROKERS setting.

    PREFER_BROKERS is a comma-separated list, e.g. "Dhan,Fyers"; order defines
    priority. Defaults to both brokers when unset.
    """

    MANAGERS: dict[str, type[IDataManager]] = {
        "dhan": DhanDataManager,
        "fyers": FyersDataManager,
    }

    @staticmethod
    def names() -> list[str]:
        raw = os.getenv("PREFER_BROKERS", "Dhan,Fyers")
        return [b.strip().lower() for b in raw.split(",") if b.strip()]

    @classmethod
    def managers(cls, auth: Optional[BrokerAuth] = None) -> dict[str, IDataManager]:
        auth = auth or BrokerAuth()
        result: dict[str, IDataManager] = {}
        for name in cls.names():
            manager_cls = cls.MANAGERS.get(name)
            if manager_cls is None:
                logger.warning("Unknown broker '%s' in PREFER_BROKERS; skipping", name)
                continue
            result[name] = manager_cls(auth)
        return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    instrument = InstrumentResolver.resolve("SBIN")
    end_date = time.strftime("%Y-%m-%d")
    start_date = time.strftime("%Y-%m-%d", time.localtime(time.time() - 5 * 86400))

    for name, manager in PreferredBrokers.managers().items():
        try:
            quote = manager.get_price(instrument)
            print(f"[OK] {name.capitalize()} quote for SBIN: {quote}")
        except Exception as e:
            print(f"[ERROR] {name.capitalize()} quote fetch failed: {e}")

        try:
            history = manager.get_historical_data(instrument, "1D", "EQUITY", start_date, end_date)
            print(f"[OK] {name.capitalize()} history (last 5 days) for SBIN: {history}")
        except Exception as e:
            print(f"[ERROR] {name.capitalize()} history fetch failed: {e}")

        try:
            chain = manager.get_option_chain(instrument, strikecount=10, weekly_expiry_count=6, monthly_expiry_count=3)
            print(f"[OK] {name.capitalize()} option chain for SBIN: {chain}")
        except Exception as e:
            print(f"[ERROR] {name.capitalize()} option chain fetch failed: {e}")


if __name__ == "__main__":
    main()
