import logging
import math
import time
from datetime import datetime
import pytz

import brokers.broker_data_manager as bdm
from utils.blob_utils import BlobUtils
from utils.market_times import MarketTimes
from storage.parquet_cache_manager import ParquetCacheManager
from storage.market_data_cache import OptionChainCacheManager
from utils.env_config import EnvConfig
from utils.shared_cache import EXPIRIES_CACHE
from schedulers.daily_progress import DailyProgress, STATUS_DONE
from schedulers.market_calendar import MarketCalendar
from schedulers.scheduler_utils import build_scheduler_context
from schedulers.live_scheduler import LiveScheduler
from services.market_data_backup_service import MarketDataBackupService


class DailyBudgetExceeded(Exception):
    """Raised inside a symbol's work when the invocation's time budget is used up."""


class DailyScheduler:
    def __init__(self, history_days: int | None = None, history_interval: str | None = None):
        # default to env-configured values when None
        self.history_days = history_days if history_days is not None else EnvConfig.history_days()
        self.history_interval = history_interval if history_interval is not None else EnvConfig.history_interval()
        self._deadline: float | None = None

    def _out_of_time(self) -> bool:
        now = datetime.now(pytz.timezone(MarketTimes.timezone()))
        return (
            now.time() > MarketTimes.close()
            or (EnvConfig.test_mode() and not EnvConfig.is_test_mode_active(now))
            or (self._deadline is not None and time.monotonic() >= self._deadline)
        )

    @staticmethod
    def _run_key(now: datetime) -> str:
        """One progress record per day; in TEST_MODE per app start, so a restart runs the job again."""
        key = now.strftime("%Y-%m-%d")
        started = EnvConfig.get_test_mode_start()
        if EnvConfig.test_mode() and started is not None:
            key += "_test_" + started.strftime("%H%M%S")
        return key

    def run(self, time_budget_seconds: float | None = None) -> bool:
        """Fetch history and option chains for every configured symbol.

        Runs pre-market (history does not need a live session, and the option-chain task only
        prefetches expiries/a baseline snapshot for LiveScheduler to reuse once the market opens).
        With `time_budget_seconds` the run is resumable: progress is recorded per symbol, work stops cleanly
        when the budget is used up, and the next invocation continues with the remaining symbols. Returns True
        when nothing is left to do for today.
        """
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        now = datetime.now(IST_TZ)

        if now.time() > MarketTimes.close():
            logging.info("[DailyScheduler] Market has closed. Skipping daily job.")
            return True
        if EnvConfig.test_mode() and not EnvConfig.is_test_mode_active(now):
            logging.info("[DailyScheduler] TEST_MODE window ended. Skipping daily job.")
            return True
        if MarketCalendar.is_holiday(now):
            logging.info("Market holiday. Skipping daily job.")
            # Still prune old broker-master folders even when the rest of the job is skipped
            bdm.MasterFileCache.cleanup_old_dated_folders()
            return True

        symbols = EnvConfig.watchlist_symbols()
        option_chain_symbols = EnvConfig.option_chain_symbols()

        ctx = build_scheduler_context(now, MarketCalendar.is_holiday, BlobUtils.is_running_locally())
        test_cache_dir = ctx.test_cache_root if ctx.use_test_cache else None
        if ctx.use_test_cache:
            logging.info("[DailyScheduler] TEST_MODE: writing cache to %s", ctx.test_cache_root)

        progress: DailyProgress | None = None
        self._deadline = None
        if time_budget_seconds is not None:
            container = BlobUtils.test_mode_cache_blob() if ctx.use_test_cache else BlobUtils.market_data_cache_blob()
            progress = DailyProgress(self._run_key(now), container=container, local_dir=test_cache_dir)
            all_tasks = [f"history:{s}" for s in symbols] + [f"chain:{s}" for s in option_chain_symbols]
            remaining = progress.pending(all_tasks)
            if not remaining:
                logging.info("[DailyScheduler] Daily job already complete for %s; nothing to do", progress.run_key)
                return True
            logging.info(
                "[DailyScheduler] Resuming daily job %s: %d of %d tasks remaining (budget %.0fs)",
                progress.run_key, len(remaining), len(all_tasks), time_budget_seconds,
            )
            self._deadline = time.monotonic() + time_budget_seconds

        candle_cache = ParquetCacheManager(cache_dir=test_cache_dir)
        chain_cache = OptionChainCacheManager(cache_dir=test_cache_dir)

        end_date = datetime.now()
        ranges = MarketCalendar.get_trading_days_back(end_date, days_back=self.history_days)

        complete = True
        try:
            for symbol in symbols:
                if self._run_task(progress, f"history:{symbol}", "History", symbol,
                                  lambda s=symbol: self._sync_history_for_symbol(s, ranges, candle_cache)) is None:
                    complete = False
                    break

            if complete:
                for symbol in option_chain_symbols:
                    if self._run_task(progress, f"chain:{symbol}", "Option chain", symbol,
                                      lambda s=symbol: self._sync_option_chain_for_symbol(s, chain_cache)) is None:
                        complete = False
                        break
        finally:
            self._deadline = None

        if complete:
            logging.info("=== Daily Job Completed ===")
        else:
            logging.warning("=== Daily Job paused: time budget or collection window ended ===")
        return complete

    def run_backup(self) -> bool:
        """Zip the day's market-data container and send it to Telegram, once per trading day.

        Returns True when the backup is done (now or earlier today) or not needed, False when it failed.
        """
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        now = datetime.now(IST_TZ)

        if not EnvConfig.test_mode() and MarketCalendar.is_holiday(now):
            logging.info("[DailyScheduler] Market holiday. Skipping backup.")
            return True

        if EnvConfig.test_mode():
            if EnvConfig.get_test_mode_expiry() is None or EnvConfig.is_test_mode_active(now):
                logging.info("[DailyScheduler] TEST_MODE window still open; backup waits until it ends")
                return True
            # The live-vs-test cache decision no longer applies once the window ends; never touch the live container.
            container = BlobUtils.test_mode_cache_blob()
        else:
            container = BlobUtils.market_data_cache_blob()

        mode = "test" if EnvConfig.test_mode() else "live"
        progress = DailyProgress(f"{now:%Y-%m-%d}_{mode}_backup", container="scheduler-state", strict=True)
        task = f"backup:{container}"
        if progress.is_finished(task):
            logging.info("[DailyScheduler] Backup already handled for %s; nothing to do", progress.run_key)
            return progress.tasks.get(task) == STATUS_DONE

        progress.record(task, False)
        try:
            backup_service = MarketDataBackupService(container_name=container)
            result = backup_service.run()
            logging.info(
                "[DailyScheduler] Backup sent: container=%s blobs=%d zip_bytes=%d telegram_message_ids=%s",
                result.container, result.blob_count, result.zip_bytes, result.telegram_message_ids,
            )
            success = True
        except Exception as ex:
            logging.exception(f"[DailyScheduler] Backup failed for container {container}: {ex!r}")
            success = False
        if success:
            progress.record(task, True)

        if not success:
            logging.info("[DailyScheduler] Container deletion skipped: backup failed; container=%s", container)
            if progress.is_finished(task):
                logging.error("[DailyScheduler] Backup stopped after %d attempts; container=%s", progress.MAX_ATTEMPTS, container)
        elif not EnvConfig.delete_after_backup():
            logging.info("[DailyScheduler] Container deletion skipped: DELETE_AFTER_BACKUP=false; container=%s", container)
        else:
            if BlobUtils.is_running_locally():
                logging.warning("[DailyScheduler] DELETE_AFTER_BACKUP ignored on a local run; container %s kept", container)
            else:
                logging.info("[DailyScheduler] Deleting backed-up container: DELETE_AFTER_BACKUP=true; container=%s", container)
                for attempt in range(1, progress.MAX_ATTEMPTS + 1):
                    try:
                        deleted = backup_service.delete_source_container()
                    except Exception:
                        logging.exception("[DailyScheduler] Container deletion attempt %d failed; container=%s", attempt, container)
                        deleted = False
                    if deleted:
                        try:
                            message_id = backup_service.notify_container_deleted(result)
                            logging.info(
                                "[DailyScheduler] Container deletion notification sent: container=%s telegram_message_id=%d",
                                container, message_id,
                            )
                        except Exception:
                            logging.exception("[DailyScheduler] Container %s deleted but Telegram notification failed", container)
                        break
                else:
                    logging.error("[DailyScheduler] Container deletion stopped after %d attempts; container=%s", progress.MAX_ATTEMPTS, container)
        return success

    def _run_task(self, progress, task: str, label: str, symbol: str, work) -> bool | None:
        """Run one symbol's work. Returns None when the time budget is used up (caller must stop).

        A failure for one symbol must never stop the remaining symbols.
        """
        if progress is not None and progress.is_finished(task):
            return True
        if self._out_of_time():
            return None
        try:
            result = work()
            success = result is not False
        except DailyBudgetExceeded:
            return None
        except Exception as ex:
            logging.exception(f"[DailyScheduler] {label} sync failed for {symbol}; continuing with next symbol: {ex!r}")
            success = False
        if progress is not None:
            progress.record(task, success)
        return success

    def _sync_history_for_symbol(self, symbol, ranges, candle_cache) -> bool:
        try:
            instrument = bdm.InstrumentResolver.resolve(symbol)
        except Exception as ex:
            logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}: {ex!r}")
            return False
        succeeded = False
        for broker_name, manager in bdm.PreferredBrokers.managers().items():
            any_ok = False
            for seg_start, seg_end in ranges:
                if self._out_of_time():
                    # Ranges already cached are skipped on the next run, so this symbol resumes cheaply.
                    raise DailyBudgetExceeded(symbol)
                ok = self._fetch_history_if_missing(
                    manager, broker_name, instrument, candle_cache, seg_start.to_pydatetime(), seg_end.to_pydatetime()
                )
                any_ok = any_ok or bool(ok)
            if not any_ok:
                logging.warning(
                    f"[DailyScheduler] {instrument.symbol} ({broker_name}) history fetch returned no data or failed for all ranges"
                )
            succeeded = succeeded or any_ok
        return succeeded

    def _sync_option_chain_for_symbol(self, symbol, chain_cache) -> bool:
        IST_TZ = pytz.timezone(MarketTimes.timezone())
        try:
            instrument = bdm.InstrumentResolver.resolve(symbol)
        except Exception as ex:
            logging.exception(f"[DailyScheduler] Failed to resolve instrument for {symbol}: {ex!r}")
            return False

        saved = False
        for broker_name, manager in bdm.PreferredBrokers.managers().items():
            try:
                today = datetime.now(IST_TZ).strftime("%Y-%m-%d")
                cache_key = (broker_name, instrument.symbol, today)
                expiries = LiveScheduler._expiries_for(manager, broker_name, instrument) or []
                if expiries:
                    EXPIRIES_CACHE[cache_key] = expiries
                else:
                    logging.warning(
                        f"[DailyScheduler] {instrument.symbol} ({broker_name}) expiries not resolved during pre-market prefetch; running here and LiveScheduler will fetch on demand"
                    )
            except Exception as ex:
                logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) failed to prefetch expiries: {ex!r}")

            saved = bool(self._fetch_option_chain(manager, broker_name, instrument, chain_cache)) or saved
        # Keep only today's dated folder in broker-master; drop prior days' master CSVs
        bdm.MasterFileCache.cleanup_old_dated_folders()

        return saved

    def _fetch_history_if_missing(self, manager, broker_name, instrument, candle_cache, start_date, end_date) -> bool:
        try:
            cached = candle_cache.load_candles(instrument.symbol, start_date, end_date, interval=self.history_interval)
            if cached:
                logging.info(
                    f"[DailyScheduler] {instrument.symbol} ({broker_name}) history already cached "
                    f"({len(cached)} candles); skipping fetch"
                )
                return True

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
    def _fetch_option_chain(manager, broker_name, instrument, chain_cache) -> bool:
        """Single strikecount=10 snapshot - no expiry-range resolution, unlike LiveScheduler.

        Returns True only when rows were saved, so an empty or failed snapshot can be retried.
        """
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
            if not rows:
                logging.warning(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain had no rows to cache")
                return False
            logging.info(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain cached ({rows} rows)")
            return True
        except Exception as ex:
            logging.exception(f"[DailyScheduler] {instrument.symbol} ({broker_name}) option chain fetch failed: {ex!r}")
            return False
