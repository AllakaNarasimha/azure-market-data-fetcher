from dataclasses import dataclass
import os
import json
import logging
from typing import Optional
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

IST_TZ = pytz.timezone(MarketTimes.timezone())
IS_RUNNING_LOCALLY_AT_START = is_running_locally()
MARKET_OPEN_TIME = MarketTimes.open()
MARKET_CLOSE_TIME = MarketTimes.close()


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


# Shared helper for scheduler functions to avoid duplicated time-window logic
@dataclass
class SchedulerContext:
    now: datetime
    test_mode_active: bool
    use_test_cache: bool
    test_cache_root: Optional[str]   


def _build_scheduler_context() -> SchedulerContext:
    """Build a consistent scheduler context used by both schedulers."""
    # Build a small, run-specific context. Static values (timezone, market
    # open/close hour) are hoisted to module-level constants.
    now = datetime.now(IST_TZ)
    test_mode_active = EnvConfig.is_test_mode_active(now)

    use_test_cache, test_cache_root = should_use_test_cache(
        test_mode_active, now=now, is_holiday=MarketCalendar.is_holiday, is_local=IS_RUNNING_LOCALLY_AT_START
    )

    return SchedulerContext(now=now, test_mode_active=test_mode_active, use_test_cache=use_test_cache, test_cache_root=test_cache_root)

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
        ctx = _build_scheduler_context()

        # Only allow scheduler execution when within market hours OR when the
        # TEST_MODE startup window is active. After the window expires, TEST_MODE
        # behavior will not permit runs outside market hours.
        market_open = ctx.now.replace(
            hour=MARKET_OPEN_TIME.hour, minute=MARKET_OPEN_TIME.minute, second=0, microsecond=0
        )
        market_close = ctx.now.replace(
            hour=MARKET_CLOSE_TIME.hour, minute=MARKET_CLOSE_TIME.minute, second=0, microsecond=0
        )

        if not ctx.use_test_cache and not (
            ctx.test_mode_active or (market_open <= ctx.now <= market_close) and not MarketCalendar.is_holiday(ctx.now)
        ):
            # If not in test-mode and market is closed/holiday, skip.
            # The above condition preserves prior behavior when test_mode is False.
            logging.info("Market is closed or holiday. Skipping.")
            return

        logging.info(f"=== Fetching Option Chains (Local Mode: {IS_RUNNING_LOCALLY_AT_START}) ===")

        if ctx.use_test_cache:
            chain_cache = OptionChainCacheManager(cache_dir=ctx.test_cache_root)
            logging.info(f"TEST_MODE after-hours: writing cache to %s", ctx.test_cache_root)
        else:
            chain_cache = OptionChainCacheManager()

        for symbol in OPTION_CHAIN_SYMBOLS:
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
        ctx = _build_scheduler_context()

        market_open = ctx.now.replace(
            hour=MARKET_OPEN_TIME.hour, minute=MARKET_OPEN_TIME.minute, second=0, microsecond=0
        )
        market_close = ctx.now.replace(
            hour=MARKET_CLOSE_TIME.hour, minute=MARKET_CLOSE_TIME.minute, second=0, microsecond=0
        )

        if not ctx.use_test_cache and not (
            ctx.test_mode_active or (market_open <= ctx.now <= market_close) and not MarketCalendar.is_holiday(ctx.now)
        ):
            logging.info("Market is closed or holiday. Skipping daily job.")
            return

        symbols = WATCHLIST_SYMBOLS
        option_chain_symbols = OPTION_CHAIN_SYMBOLS
        
        if ctx.use_test_cache:
            candle_cache = ParquetCacheManager(cache_dir=ctx.test_cache_root)
            chain_cache = OptionChainCacheManager(cache_dir=ctx.test_cache_root)
            logging.info(f"TEST_MODE after-hours: writing daily cache to %s", ctx.test_cache_root)
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

logging.info(f"Startup env: TEST_MODE={EnvConfig.env('TEST_MODE')!r}, WEBSITE_SITE_NAME={EnvConfig.website_site_name()!r}")
# Initialize a short-lived TEST_MODE window at process start (if enabled).
if EnvConfig.test_mode():
    IST_TZ = pytz.timezone(MarketTimes.timezone())
    EnvConfig.init_test_mode_window(datetime.now(IST_TZ))

OPTION_CHAIN_SYMBOLS = EnvConfig.option_chain_symbols()
WATCHLIST_SYMBOLS = EnvConfig.watchlist_symbols()

# TEST_MODE must tick on weekends so the five-minute window can close and log completion.
_live_schedule = "0 * * * * *" if EnvConfig.test_mode() else "0 * * * * 1-5"
@app.schedule(schedule=_live_schedule, arg_name="mytimer", run_on_startup=EnvConfig.test_mode(), use_monitor=False)
def market_data_fetcher(mytimer: func.TimerRequest) -> None:
    LiveScheduler().run()

# Daily job runs at 8:30 AM IST on weekdays
@app.schedule(schedule="0 0 3 * * 1-5", arg_name="dailyTimer", run_on_startup=EnvConfig.test_mode(), use_monitor=False)
def daily_job(dailyTimer: func.TimerRequest) -> None:
    DailyScheduler().run()


