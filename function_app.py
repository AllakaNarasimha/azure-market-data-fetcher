import logging
from datetime import datetime
import azure.functions as func

from utils.blob_utils import BlobUtils
from storage.market_data_storage_client import MarketDataStorageClient
from schedulers.live_scheduler import LiveScheduler
from schedulers.daily_scheduler import DailyScheduler
from app_settings import (
    IST_TZ,
    TEST_MODE,
    OPTION_CHAIN_SYMBOLS,
    HISTORY_DAYS,
    HISTORY_INTERVAL,
    PARALLEL_BATCH_SIZE,
    DYNAMIC_OPTION_CHAIN_CRON,
)

app = func.FunctionApp()

# Daily job runs at 8:30 AM IST on weekdays
@app.schedule(schedule="0 0 3 * * 1-5", arg_name="dailyTimer", run_on_startup=TEST_MODE, use_monitor=False)
def daily_job(dailyTimer: func.TimerRequest) -> None:
    try:
        DailyScheduler(history_days=HISTORY_DAYS, history_interval=HISTORY_INTERVAL).run()
    except Exception as ex:
        logging.exception(f"[daily_job] DailyScheduler.run() failed: {ex!r}")


if DYNAMIC_OPTION_CHAIN_CRON:
    @app.schedule(schedule=DYNAMIC_OPTION_CHAIN_CRON, arg_name="chainTimer", run_on_startup=TEST_MODE, use_monitor=False)
    def option_chain_fetcher(chainTimer: func.TimerRequest) -> None:
        now = datetime.now(IST_TZ)
        current_second = now.second
        target_seconds = [current_second]

        # Catch up on any seconds missed within the same minute (e.g. cold start delay).
        if chainTimer.past_due and chainTimer.schedule_status and chainTimer.schedule_status.last:
            try:
                last_run = datetime.fromisoformat(str(chainTimer.schedule_status.last))
                if last_run.minute == now.minute and last_run.second < current_second:
                    target_seconds = list(range(last_run.second + 1, current_second + 1))
                    logging.warning(f"[LiveScheduler] Timer past due; catching up seconds {target_seconds}")
            except Exception:
                logging.exception("[LiveScheduler] Failed parsing schedule_status.last")

        for sec in target_seconds:
            start_idx = sec * PARALLEL_BATCH_SIZE
            batch_symbols = OPTION_CHAIN_SYMBOLS[start_idx : start_idx + PARALLEL_BATCH_SIZE]
            if not batch_symbols:
                continue
            try:
                LiveScheduler().run(symbols=batch_symbols, batch_second=sec)
            except Exception as ex:
                logging.exception(f"[option_chain_fetcher] LiveScheduler.run() failed for second {sec}: {ex!r}")

if __name__ == "__main__":
    logging.info("Function app started")
    if BlobUtils.is_running_locally():
        logging.info("Running locally: executing DailyScheduler for debug")
        try:
            ds = DailyScheduler()
            ds.run()
            logging.info("DailyScheduler debug run completed")
        except Exception:
            logging.exception("DailyScheduler debug run failed")
        # Continue with existing local helpers (index generation) for convenience
        try:
            storage_client = MarketDataStorageClient()
            # Use recursive listing to traverse nested virtual folders and obtain
            # blob objects (not just names) so downstream helpers can use them
            folders = storage_client.list_folders_in_container(BlobUtils.MARKET_DATA_CACHE_BLOB)
            matches = []
            import fnmatch
            from pathlib import Path
            from storage.azure_blob_market_indexer import AzureBlobMarketIndexer
            for folder in folders:
                try:
                    if 'option_chain/' not in folder:
                        continue

                    blob_objs = storage_client.list_files_in_subfolder(
                        BlobUtils.MARKET_DATA_CACHE_BLOB, folder, return_blob_objects=True
                    )
                    for blob_obj in blob_objs:
                        if fnmatch.fnmatch(Path(blob_obj.name).name, 'part-0.parquet'):
                            matches.append(blob_obj)
                except Exception as e:
                    logging.warning('warning listing %s: %s', folder, e)
            logging.info("All folders in %s: %s", BlobUtils.MARKET_DATA_CACHE_BLOB, folders)
            logging.info("All parquet matches: %s", [getattr(m, 'name', str(m)) for m in matches])
            if matches:
                first_blob = matches[0]
                azure_blob_market_index = AzureBlobMarketIndexer()
                dst_index_file = azure_blob_market_index.generate_and_upload_index(first_blob)
                logging.info("Generated and uploaded index file: %s", dst_index_file)
        except Exception:
            logging.exception("Local index generation failed")

