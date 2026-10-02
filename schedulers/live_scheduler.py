import asyncio
import math
import logging
from datetime import datetime
import pytz

import brokers.broker_data_manager as bdm
from utils.blob_utils import BlobUtils
from utils.market_times import MarketTimes
from storage.market_data_cache import OptionChainCacheManager
from utils.env_config import EnvConfig
from utils.shared_cache import EXPIRIES_CACHE
from schedulers.market_calendar import MarketCalendar
from schedulers.scheduler_utils import build_scheduler_context


class LiveScheduler:
    def __init__(self, strikecount: int = 10, weeks: int = 6, months: int = 3, max_concurrency: int = 5):
        self.strikecount = strikecount
        self.weeks = weeks
        self.months = months
        self.max_concurrency = max_concurrency

    def run(self, symbols: list[str] | None = None, batch_second: int | None = None) -> None:
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        # Respect TEST_MODE/test-cache decision inside downstream helpers where needed
        if batch_second is None:
            try:
                now_sec = datetime.now(IST_TZ).second
                try:
                    # default parallel batch size from env or 5
                    pb_raw = EnvConfig.env("PARALLEL_BATCH_SIZE", "5") or "5"
                    pb = int(pb_raw)
                    required_seconds = math.ceil(len(EnvConfig.option_chain_symbols()) / pb) if pb > 0 else 0
                    batch_second = now_sec % required_seconds if required_seconds else now_sec
                except Exception:
                    batch_second = now_sec
            except Exception:
                batch_second = None

        target_symbols = symbols if symbols is not None else EnvConfig.option_chain_symbols()

        # Route to the test-mode cache root/container when TEST_MODE is active
        # and the market is closed/holiday, so after-hours test runs don't
        # pollute the production market-data-cache container.
        ctx = build_scheduler_context(datetime.now(IST_TZ), MarketCalendar.is_holiday, BlobUtils.is_running_locally())
        chain_cache = OptionChainCacheManager(cache_dir=ctx.test_cache_root if ctx.use_test_cache else None)
        if ctx.use_test_cache:
            logging.info("[LiveScheduler] TEST_MODE after-hours: writing cache to %s", ctx.test_cache_root)

        asyncio.run(self._run_async(target_symbols, chain_cache, batch_second))

    async def _run_async(self, symbols: list[str], chain_cache, batch_second: int | None = None) -> None:
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def _process_symbol(symbol: str) -> None:
            async with semaphore:
                try:
                    instrument = await asyncio.to_thread(bdm.InstrumentResolver.resolve, symbol)
                except Exception as ex:
                    logging.exception(f"[LiveScheduler] Failed to resolve instrument for {symbol}: {ex!r}")
                    return

                try:
                    brokers_for_symbol = bdm.PreferredBrokers.managers().items()
                except Exception as ex:
                    logging.exception(f"[LiveScheduler] Failed to resolve preferred brokers for {symbol}: {ex!r}")
                    return

                for broker_name, manager in brokers_for_symbol:
                    try:
                        await asyncio.to_thread(
                            self._fetch_option_chain_range, manager, broker_name, instrument, chain_cache, batch_second
                        )
                    except Exception as ex:
                        # A single broker/symbol failure must not cancel the rest of
                        # asyncio.gather's in-flight tasks (which would fail the whole
                        # Azure Functions invocation).
                        logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain batch failed: {ex!r}")

        await asyncio.gather(*(_process_symbol(symbol) for symbol in symbols))

    @staticmethod
    def _expiries_for(manager, broker_name: str, instrument) -> list:
        if broker_name == "fyers":
            return manager.get_expiries(instrument)
        if broker_name == "dhan":
            return bdm.ExpiryResolver.classify_dates(manager.get_expiry_dates(instrument))
        return []

    def _fetch_option_chain_range(self, manager, broker_name, instrument, chain_cache, batch_second: int | None = None) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        cache_key = (broker_name, instrument.symbol, today)
        expiries = EXPIRIES_CACHE.get(cache_key)
        if expiries is None:
            try:
                expiries = self._expiries_for(manager, broker_name, instrument) or []
                EXPIRIES_CACHE[cache_key] = expiries
            except Exception as ex:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) failed to fetch expiries: {ex!r}")
                return

        expiry_list = []
        try:
            expiry_list = bdm.ExpiryResolver.resolve_range(expiries, weeks=self.weeks, months=self.months)
        except Exception as ex:
            # Malformed/unexpected expiry entries (missing keys, wrong shape, etc.)
            # must not abort the whole batch's asyncio.gather - log and skip this symbol.
            logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) failed to resolve expiry range: {ex!r}")
            return

        responses: list[tuple] = []
        for expiry_ts in expiry_list:
            try:
                chain = manager.get_option_chain(instrument, strikecount=self.strikecount, expiries=expiry_ts)
                responses.append((chain, expiry_ts))
                logging.info(f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain fetched for expiry {expiry_ts}")
            except Exception as ex:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain fetch failed for expiry {expiry_ts}: {ex!r}")

        if responses:
            try:
                rows = chain_cache.save_fyers_responses_batch(
                    instrument.symbol, responses, source=broker_name, fetched_at=None, batch_second=batch_second
                )
                logging.info(
                    f"[LiveScheduler] {instrument.symbol} ({broker_name}) option chain cached "
                    f"({rows} rows across {len(responses)} expiries)"
                )
            except Exception as ex:
                logging.exception(f"[LiveScheduler] {instrument.symbol} ({broker_name}) failed to save batched option chain: {ex!r}")
