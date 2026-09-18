import os
import json
import logging
import pytz
from datetime import datetime, timedelta
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
import azure.functions as func

import broker_data_manager as bdm
from parquet_cache_manager import ParquetCacheManager
from market_data_cache import OptionChainCacheManager

# When running the module directly (not in Azure), load local.settings.json
# into environment variables so values like TEST_MODE are available.
try:
    if os.getenv("WEBSITE_INSTANCE_ID") is None and os.path.exists("local.settings.json"):
        with open("local.settings.json", "r") as _f:
            _cfg = json.load(_f)
            for _k, _v in _cfg.get("Values", {}).items():
                if os.getenv(_k) is None:
                    os.environ[_k] = str(_v)
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
        is_local = os.getenv("WEBSITE_INSTANCE_ID") is None
        test_mode = os.getenv("TEST_MODE", "false").strip().lower() == "true"

        ist_tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist_tz)

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        if not (market_open <= now <= market_close) and not test_mode:
            logging.info("Market is closed. Skipping.")
            return
        if MarketCalendar.is_holiday(now) and not test_mode:
            logging.info("Market holiday. Skipping.")
            return

        logging.info(f"=== Fetching Option Chains (Local Mode: {is_local}) ===")

        option_chain_symbols = [s.strip() for s in os.getenv("OPTION_CHAIN_SYMBOLS", "").split(",") if s.strip()]
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

        for week in range(self.weeks):
            try:
                expiry_ts = bdm.ExpiryResolver.resolve(expiries, weekly_expiry_count=week)
            except bdm.ExpiryNotAvailable:
                logging.info(f"[LiveScheduler] {instrument.symbol} ({broker_name}) only {week} weekly expiries available")
                break
            try:
                chain = manager.get_option_chain(instrument, strikecount=self.strikecount, weekly_expiry_count=week)
                rows = chain_cache.save_fyers_response(
                    instrument.symbol, chain, source=broker_name, expiry_timestamp=expiry_ts
                )
                logging.info(
                    f"[LiveScheduler] {instrument.symbol} ({broker_name}) weekly+{week} option chain cached ({rows} rows)"
                )
            except Exception:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) weekly+{week} option chain fetch failed")

        for month in range(self.months):
            try:
                expiry_ts = bdm.ExpiryResolver.resolve(expiries, monthly_expiry_count=month)
            except bdm.ExpiryNotAvailable:
                logging.info(f"[LiveScheduler] {instrument.symbol} ({broker_name}) only {month} monthly expiries available")
                break
            try:
                chain = manager.get_option_chain(instrument, strikecount=self.strikecount, monthly_expiry_count=month)
                rows = chain_cache.save_fyers_response(
                    instrument.symbol, chain, source=broker_name, expiry_timestamp=expiry_ts
                )
                logging.info(
                    f"[LiveScheduler] {instrument.symbol} ({broker_name}) monthly+{month} option chain cached ({rows} rows)"
                )
            except Exception:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) monthly+{month} option chain fetch failed")


class DailyScheduler:
    """Daily job: backfill last N-days history + snapshot option chains for the watchlist."""

    def __init__(self, history_days: int = 12):
        self.history_days = history_days

    def run(self) -> None:
        is_local = os.getenv("WEBSITE_INSTANCE_ID") is None
        logging.info(f"=== Daily Job Started (Local Mode: {is_local}) ===")

        if MarketCalendar.is_holiday(datetime.now()):
            logging.info("Market holiday. Skipping daily job.")
            return

        symbols = [s.strip() for s in os.getenv("WATCHLIST_SYMBOLS", "SBIN").split(",") if s.strip()]
        option_chain_symbols = [s.strip() for s in os.getenv("OPTION_CHAIN_SYMBOLS", "").split(",") if s.strip()]
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
_test_mode = os.getenv("TEST_MODE", "false").strip().lower() == "true"
logging.info(f"Startup env: TEST_MODE={os.getenv('TEST_MODE')!r}, WEBSITE_INSTANCE_ID={os.getenv('WEBSITE_INSTANCE_ID')!r}")

@app.timer_trigger(schedule="0 * * * * 1-5", arg_name="mytimer", run_on_startup=_test_mode, use_monitor=False)
def market_data_fetcher(mytimer: func.TimerRequest) -> None:
    LiveScheduler().run()


@app.timer_trigger(schedule="0 30 8 * * 1-5", arg_name="dailyTimer", run_on_startup=_test_mode, use_monitor=False)
def daily_job(dailyTimer: func.TimerRequest) -> None:
    DailyScheduler().run()


