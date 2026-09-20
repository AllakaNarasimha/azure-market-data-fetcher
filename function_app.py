import os
import json
import logging
import pytz
from datetime import datetime, timedelta
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
import azure.functions as func

import broker_data_manager as bdm
from broker_authenticate import is_running_locally
from parquet_cache_manager import ParquetCacheManager
from market_data_cache import OptionChainCacheManager
from market_times import MarketTimes
from cache_utils import should_use_test_cache
from env_config import EnvConfig

# When running the module directly (not in Azure), load local.settings.json
# into environment variables so values like TEST_MODE are available.
try:
    if is_running_locally() and os.path.exists("local.settings.json"):
        with open("local.settings.json", "r") as _f:
            _cfg = json.load(_f)
            for _k, _v in _cfg.get("Values", {}).items():
                if EnvConfig.env(_k) is None:
                    EnvConfig.set_override(_k, str(_v))
        logging.info("Loaded local.settings.json into environment for local run")
except Exception:
    logging.exception("Failed to load local.settings.json into environment")

app = func.FunctionApp()


class MarketCalendar:
    """Manages market trading days, weekends, and specific exchange holidays.

    Used for properly aligning lookback windows to actual trading sessions.
    """

    NSE_HOLIDAYS = [
        "2026-01-15",
        "2026-01-26",
        "2026-03-03",
        "2026-03-26",
        "2026-03-31",
        "2026-04-03",
        "2026-04-14",
        "2026-05-01",
        "2026-05-28",
        "2026-06-26",
        "2026-09-14",
        "2026-10-02",
        "2026-10-20",
        "2026-11-10",
        "2026-11-24",
        "2026-12-25",
    ]

    DEFAULT_BDAY = CustomBusinessDay(holidays=NSE_HOLIDAYS)

    @staticmethod
    def get_trading_days_back(reference_date: datetime, days_back: int, holidays: list = None) -> pd.Timestamp:
        """Return the exact Timestamp that is `days_back` trading sessions prior to `reference_date`.

        Automatically skips weekends and exchange holidays.
        """
        ref_ts = pd.Timestamp(reference_date)
        if int(days_back) == 0:
            return ref_ts

        offset = CustomBusinessDay(holidays=holidays) if holidays is not None else MarketCalendar.DEFAULT_BDAY
        return ref_ts - (offset * int(days_back))

    @staticmethod
    def is_holiday(check_date: datetime) -> bool:
        """True if check_date is a weekend or a known NSE holiday."""
        if check_date.weekday() >= 5:
            return True
        return check_date.strftime("%Y-%m-%d") in MarketCalendar.NSE_HOLIDAYS

# ---------------------------------------------------------
# MAIN: The Core Logic
# ---------------------------------------------------------
class LiveScheduler:
    """Per-minute option chain fetcher: N strikes across weekly/monthly expiries."""

    def __init__(self, strikecount: int = 10, weeks: int = 6, months: int = 3):
        self.strikecount = strikecount
        self.weeks = weeks
        self.months = months

    def run(self) -> None:
        is_local = is_running_locally()
        # Read env var for informational use, but only enable TEST_MODE behavior
        # while the startup window is active as determined by _is_test_mode_active().
        _ = EnvConfig.test_mode()

        ist_tz = pytz.timezone(MarketTimes.timezone())
        now = datetime.now(ist_tz)
        test_mode_active = _is_test_mode_active(now)

        market_open = now.replace(
            hour=MarketTimes.open().hour, minute=MarketTimes.open().minute, second=0, microsecond=0
        )
        market_close = now.replace(
            hour=MarketTimes.close().hour, minute=MarketTimes.close().minute, second=0, microsecond=0
        )

        use_test_cache, test_cache_root = should_use_test_cache(test_mode_active, now=now, is_holiday=MarketCalendar.is_holiday, is_local=is_local)

        # Only allow scheduler execution when within market hours OR when the
        # TEST_MODE startup window is active. After the window expires, TEST_MODE
        # behavior will not permit runs outside market hours.
        if not use_test_cache and not (test_mode_active or (market_open <= now <= market_close) and not MarketCalendar.is_holiday(now)):
            # If not in test-mode and market is closed/holiday, skip.
            # The above condition preserves prior behavior when test_mode is False.
            logging.info("Market is closed or holiday. Skipping.")
            return

        logging.info(f"=== Fetching Option Chains (Local Mode: {is_local}) ===")

        option_chain_symbols = EnvConfig.option_chain_symbols()
        if use_test_cache:
            chain_cache = OptionChainCacheManager(cache_dir=test_cache_root)
            logging.info(f"TEST_MODE after-hours: writing cache to %s", test_cache_root)
        else:
            chain_cache = OptionChainCacheManager()

        for symbol in option_chain_symbols:
            try:
                instrument = bdm.InstrumentResolver.resolve(symbol)
            except Exception:
                logging.exception(f"[LiveScheduler] Failed to resolve instrument for {symbol}")
                continue

            for broker_name, manager in bdm.PreferredBrokers.managers().items():
                self._fetch_option_chain_range(manager, broker_name, instrument, chain_cache)

        logging.info("=== Execution Completed ===")

    @staticmethod
    def _expiries_for(manager, broker_name: str, instrument) -> list:
        if broker_name == "fyers":
            return manager.get_expiries(instrument)
        if broker_name == "dhan":
            return bdm.ExpiryResolver.classify_dates(manager.get_expiry_dates(instrument))
        return []

    def _fetch_option_chain_range(self, manager, broker_name, instrument, chain_cache) -> None:
        try:
            expiries = self._expiries_for(manager, broker_name, instrument)
        except Exception:
            logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) failed to fetch expiries")
            return

        # Resolve a deduplicated, sorted list of expiries for up to `weeks` and `months`
        expiry_list = bdm.ExpiryResolver.resolve_range(expiries, weeks=self.weeks, months=self.months)

        responses: list[tuple] = []
        for expiry_ts in expiry_list:
            try:
                chain = manager.get_option_chain(instrument, strikecount=self.strikecount, expiries=expiry_ts)
                responses.append((chain, expiry_ts))
                logging.info(f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain fetched for expiry {expiry_ts}")
            except Exception:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain fetch failed for expiry {expiry_ts}")

        if responses:
            try:
                rows = chain_cache.save_fyers_responses_batch(instrument.symbol, responses, source=broker_name)
                logging.info(
                    f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain cached "
                    f"({rows} rows across {len(responses)} expiries)"
                )
            except Exception:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) failed to save batched option chain")



class DailyScheduler:
    """Daily job: backfill last N-days history + snapshot option chains for the watchlist."""

    def __init__(self, history_days: int = 12):
        self.history_days = history_days

    def run(self) -> None:
        is_local = is_running_locally()
        _ = EnvConfig.test_mode()
        ist_tz = pytz.timezone(MarketTimes.timezone())
        now = datetime.now(ist_tz)
        test_mode_active = _is_test_mode_active(now)

        market_open = now.replace(
            hour=MarketTimes.open().hour, minute=MarketTimes.open().minute, second=0, microsecond=0
        )
        market_close = now.replace(
            hour=MarketTimes.close().hour, minute=MarketTimes.close().minute, second=0, microsecond=0
        )

        use_test_cache, test_cache_root = should_use_test_cache(test_mode_active, now=now, is_holiday=MarketCalendar.is_holiday, is_local=is_local)

        if not use_test_cache and not (test_mode_active or (market_open <= now <= market_close) and not MarketCalendar.is_holiday(now)):
            logging.info("Market is closed or holiday. Skipping daily job.")
            return

        symbols = EnvConfig.watchlist_symbols()
        option_chain_symbols = EnvConfig.option_chain_symbols()
        if use_test_cache:
            candle_cache = ParquetCacheManager(cache_dir=test_cache_root)
            chain_cache = OptionChainCacheManager(cache_dir=test_cache_root)
            logging.info(f"TEST_MODE after-hours: writing daily cache to %s", test_cache_root)
        else:
            candle_cache = ParquetCacheManager()
            chain_cache = OptionChainCacheManager()
        end_date = datetime.now()
        start_date = MarketCalendar.get_trading_days_back(end_date, days_back=self.history_days).to_pydatetime()

        for symbol in symbols:
            try:
                instrument = bdm.InstrumentResolver.resolve(symbol)
            except Exception:
                logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}")
                continue

            for broker_name, manager in bdm.PreferredBrokers.managers().items():
                self._fetch_history_if_missing(manager, broker_name, instrument, candle_cache, start_date, end_date)

        for symbol in option_chain_symbols:
            try:
                instrument = bdm.InstrumentResolver.resolve(symbol)
            except Exception:
                logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}")
                continue

            for broker_name, manager in bdm.PreferredBrokers.managers().items():
                self._fetch_option_chain(manager, broker_name, instrument, chain_cache)

        logging.info("=== Daily Job Completed ===")

    @staticmethod
    def _fetch_history_if_missing(manager, broker_name, instrument, candle_cache, start_date, end_date) -> None:
        try:
            cached = candle_cache.load_candles(instrument.symbol, start_date, end_date, interval="1D")
            if cached:
                logging.info(
                    f"[DailyScheduler] {instrument.symbol} ({broker_name}) history already cached "
                    f"({len(cached)} candles); skipping fetch"
                )
                return

            response = manager.get_historical_data(
                instrument, "1D", "EQUITY", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
            candles = bdm.HistoryNormalizer.to_candles(broker_name, response)
            if not candles:
                logging.warning(f"[DailyScheduler] {instrument.symbol} ({broker_name}) history response had no candles")
                return

            candle_cache.clean_and_save_candles(
                instrument.symbol, candles, interval="1D", fetch_start=start_date, fetch_end=end_date
            )
            logging.info(
                f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetched & cached ({len(candles)} candles)"
            )
        except Exception:
            logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetch failed")

    @staticmethod
    def _fetch_option_chain(manager, broker_name, instrument, chain_cache) -> None:
        try:
            chain = manager.get_option_chain(instrument, strikecount=10)
            rows = chain_cache.save_fyers_response(instrument.symbol, chain, source=broker_name)
            logging.info(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain cached ({rows} rows)")
        except Exception:
            logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain fetch failed")

# TEST_MODE runs both schedulers once when the host starts, via the SDK's
# supported run_on_startup mechanism. Do NOT execute scheduler logic at
# module import time - the Functions worker imports this module to index
# functions, and synchronous broker/network calls during that phase crash
# the host with "Value cannot be null. (Parameter 'provider')".
_test_mode = EnvConfig.test_mode()
logging.info(f"Startup env: TEST_MODE={EnvConfig.env('TEST_MODE')!r}, WEBSITE_SITE_NAME={EnvConfig.website_site_name()!r}")
# Establish a short lived TEST_MODE window at process start.
# When TEST_MODE is enabled we allow scheduler runs for a fixed window
# (default 5 minutes) starting at module import (i.e., host start).
IST_TZ = pytz.timezone(MarketTimes.timezone())
_test_mode_env = EnvConfig.test_mode()
_test_mode_minutes = EnvConfig.test_mode_minutes()
_TEST_MODE_START = None
_TEST_MODE_EXPIRY = None
_test_mode_completed_logged = False
if _test_mode_env:
    _TEST_MODE_START = datetime.now(IST_TZ)
    _TEST_MODE_EXPIRY = _TEST_MODE_START + timedelta(minutes=_test_mode_minutes)
    logging.info("TEST_MODE enabled: running for %s minutes until %s", _test_mode_minutes, _TEST_MODE_EXPIRY.isoformat())


def _is_test_mode_active(now: datetime) -> bool:
    """Return True when TEST_MODE is enabled and still within the startup window."""
    global _test_mode_completed_logged
    if not _test_mode_env:
        return False
    if _TEST_MODE_EXPIRY is None:
        return False
    if now <= _TEST_MODE_EXPIRY:
        return True
    if not _test_mode_completed_logged:
        logging.info(
            "TEST_MODE window completed: ran for %s minutes, expired at %s",
            _test_mode_minutes, _TEST_MODE_EXPIRY.isoformat(),
        )
        _test_mode_completed_logged = True
    return False

# Live job runs at every minute of every hour
@app.schedule(schedule="0 * * * * 1-5", arg_name="mytimer", run_on_startup=_test_mode_env, use_monitor=False)
def market_data_fetcher(mytimer: func.TimerRequest) -> None:
    LiveScheduler().run()

# Daily job runs at 8:30 AM IST on weekdays
@app.schedule(schedule="0 0 3 * * 1-5", arg_name="dailyTimer", run_on_startup=_test_mode_env, use_monitor=False)
def daily_job(dailyTimer: func.TimerRequest) -> None:
    DailyScheduler().run()


