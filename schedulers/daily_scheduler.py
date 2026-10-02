import logging
import math
from datetime import datetime
import pytz

import brokers.broker_data_manager as bdm
from utils.market_times import MarketTimes
from storage.parquet_cache_manager import ParquetCacheManager
from storage.market_data_cache import OptionChainCacheManager
from utils.env_config import EnvConfig
from utils.shared_cache import EXPIRIES_CACHE
from schedulers.market_calendar import MarketCalendar
from schedulers.live_scheduler import LiveScheduler


class DailyScheduler:
    def __init__(self, history_days: int | None = None, history_interval: str | None = None):
        # default to env-configured values when None
        self.history_days = history_days if history_days is not None else EnvConfig.history_days()
        self.history_interval = history_interval if history_interval is not None else EnvConfig.history_interval()

    def run(self) -> None:
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        now = datetime.now(IST_TZ)

        # Build scheduler context via EnvConfig helpers when needed
        market_open = now.replace(hour=MarketTimes.open().hour, minute=MarketTimes.open().minute, second=0, microsecond=0)
        market_close = now.replace(hour=MarketTimes.close().hour, minute=MarketTimes.close().minute, second=0, microsecond=0)

        if not EnvConfig.test_mode() and not ((market_open <= now <= market_close) and not MarketCalendar.is_holiday(now)):
            logging.info("Market is closed or holiday. Skipping daily job.")
            return

        symbols = EnvConfig.watchlist_symbols()
        option_chain_symbols = EnvConfig.option_chain_symbols()

        candle_cache = ParquetCacheManager()
        chain_cache = OptionChainCacheManager()

        end_date = datetime.now()
        ranges = MarketCalendar.get_trading_days_back(end_date, days_back=self.history_days)

        for symbol in symbols:
            try:
                instrument = bdm.InstrumentResolver.resolve(symbol)
            except Exception as ex:
                logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}: {ex!r}")
                continue
            for broker_name, manager in bdm.PreferredBrokers.managers().items():
                any_fetched = False
                for seg_start, seg_end in ranges:
                    fetched = self._fetch_history_if_missing(
                        manager, broker_name, instrument, candle_cache, seg_start.to_pydatetime(), seg_end.to_pydatetime()
                    )
                    any_fetched = any_fetched or bool(fetched)
                if not any_fetched:
                    logging.warning(
                        f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetch returned no data or failed for all ranges"
                    )

        for symbol in option_chain_symbols:
            try:
                instrument = bdm.InstrumentResolver.resolve(symbol)
            except Exception as ex:
                logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}: {ex!r}")
                continue

            for broker_name, manager in bdm.PreferredBrokers.managers().items():
                try:
                    today = datetime.now(IST_TZ).strftime("%Y-%m-%d")
                    cache_key = (broker_name, instrument.symbol, today)
                    expiries = LiveScheduler._expiries_for(manager, broker_name, instrument) or []
                    EXPIRIES_CACHE[cache_key] = expiries
                    if not expiries:
                        logging.warning(
                            f"[DailyScheduler] {instrument.symbol} ({broker_name}) expiries not resolved during pre-market prefetch; running here and LiveScheduler will fetch on demand"
                        )
                except Exception as ex:
                    logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) failed to prefetch expiries: {ex!r}")

                self._fetch_option_chain(manager, broker_name, instrument, chain_cache)

        logging.info("=== Daily Job Completed ===")

    def _fetch_history_if_missing(self, manager, broker_name, instrument, candle_cache, start_date, end_date) -> bool:
        try:
            cached = candle_cache.load_candles(instrument.symbol, start_date, end_date, interval=self.history_interval)
            if cached:
                logging.info(
                    f"[DailyScheduler] {instrument.symbol} ({broker_name}) history already cached "
                    f"({len(cached)} candles); skipping fetch"
                )
                return False

            response = manager.get_historical_data(
                instrument, self.history_interval, "EQUITY", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
            candles = bdm.HistoryNormalizer.to_candles(broker_name, response)
            if not candles:
                logging.warning(f"[DailyScheduler] {instrument.symbol} ({broker_name}) history response had no candles")
                return False

            candle_cache.clean_and_save_candles(
                instrument.symbol, candles, interval=self.history_interval, fetch_start=start_date, fetch_end=end_date
            )
            logging.info(
                f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetched & cached ({len(candles)} candles)"
            )
            return True
        except Exception as ex:
            logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetch failed: {ex!r}")
            return False

    @staticmethod
    def _fetch_option_chain(manager, broker_name, instrument, chain_cache) -> None:
        """Single strikecount=10 snapshot - no expiry-range resolution, unlike LiveScheduler."""
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        try:
            chain = manager.get_option_chain(instrument, strikecount=10)
            now_sec = datetime.now(IST_TZ).second
            try:
                pb = int(EnvConfig.env("PARALLEL_BATCH_SIZE", "5") or "5")
                required_seconds = math.ceil(len(EnvConfig.option_chain_symbols()) / pb) if pb > 0 else 0
                batch_second = now_sec % required_seconds if required_seconds else now_sec
            except Exception:
                batch_second = now_sec
            rows = chain_cache.save_fyers_response(
                instrument.symbol, chain, source=broker_name, batch_second=batch_second
            )
            logging.info(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain cached ({rows} rows)")
        except Exception as ex:
            logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain fetch failed: {ex!r}")
