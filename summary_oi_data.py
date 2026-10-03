"""Option-chain arrival timing summary.

Gathers every stock's option-chain `part-0.parquet` cached under
`option_chain/` in the market-data-cache blob container (reusing the same
folder-walk used in `function_app.py`'s local `__main__` block), then reports
the first `fetched_at` record per stock per day so you can see the order and
time gap in which each stock's data lands (e.g. stock1 at 9:15:00, stock2 at
9:15:01, ...).
"""

from __future__ import annotations

import fnmatch
import io
import json
import logging
import os
import re
import urllib.parse
from pathlib import Path
from typing import Optional

import pandas as pd

from utils.blob_utils import BlobUtils
from utils.cache_utils import MARKET_CACHE_DIRNAME, TEST_CACHE_DIRNAME
from utils.env_config import EnvConfig
from storage.market_data_storage_client import MarketDataStorageClient

logger = logging.getLogger(__name__)

# Running this module standalone (not via `func start`) skips Azure Functions'
# automatic local.settings.json loading, so MARKET_STORAGE_CONNECTION etc.
# would otherwise be missing; mirror function_app.py's loader here too.
if BlobUtils.is_running_locally() and os.path.exists("local.settings.json"):
    try:
        with open("local.settings.json", "r") as _f:
            _cfg = json.load(_f)
        for _k, _v in _cfg.get("Values", {}).items():
            if EnvConfig.env(_k) is None:
                EnvConfig.set_override(_k, str(_v))
    except Exception:
        logger.exception("Failed to load local.settings.json into environment")

OPTION_CHAIN_PREFIX = "option_chain/"
# In the test-mode-data container, BlobSync prefixes every blob with the
# market_data_cache folder (see cache_utils.test_blob_name), so option-chain
# blobs live one level deeper than in the market-data-cache container.
TEST_OPTION_CHAIN_PREFIX = f"{MARKET_CACHE_DIRNAME}/{OPTION_CHAIN_PREFIX}"
PART_FILE_PATTERN = "part-0.parquet"
# fetched_at is stored as a naive UTC datetime (datetime.now() on the host); shift to IST for reporting.
IST_OFFSET = pd.Timedelta(hours=5, minutes=30)

def _gather_option_chain_blobs(storage_client: MarketDataStorageClient) -> list:
    """Collect every option-chain `part-0.parquet` blob across all stocks."""
    folders = storage_client.list_folders_in_container(BlobUtils.market_data_cache_blob())
    matches = []
    for folder in folders:
        if OPTION_CHAIN_PREFIX not in folder:
            continue
        try:
                blob_objs = storage_client.list_files_in_subfolder(
                BlobUtils.market_data_cache_blob(), folder, return_blob_objects=True
            )
        except Exception:
            logger.exception("Failed listing option-chain blobs under '%s'", folder)
            continue

        for blob_obj in blob_objs:
            if fnmatch.fnmatch(Path(blob_obj.name).name, PART_FILE_PATTERN):
                matches.append(blob_obj)
    return matches


def _gather_test_option_chain_blobs(storage_client: MarketDataStorageClient) -> list:
    """Collect every option-chain `part-0.parquet` blob from the test-mode-data container."""
    try:
        blob_objs = storage_client.list_files_in_subfolder(
            BlobUtils.test_mode_cache_blob(), TEST_OPTION_CHAIN_PREFIX, return_blob_objects=True
        )
    except Exception:
        logger.exception("Failed listing option-chain blobs under '%s'", TEST_OPTION_CHAIN_PREFIX)
        return []

    return [blob_obj for blob_obj in blob_objs if fnmatch.fnmatch(Path(blob_obj.name).name, PART_FILE_PATTERN)]


def _extract_underlying_symbol(blob_name: str) -> str:
    """Pull the decoded stock/underlying name out of a `.../underlying=<enc>/...` blob path."""
    match = re.search(r"underlying=([^/\\]+)", blob_name)
    encoded = match.group(1) if match else "UNKNOWN"
    return urllib.parse.unquote(encoded)


def _read_option_chain_parquet(storage_client: MarketDataStorageClient, blob_obj) -> pd.DataFrame:
    """Load a single option-chain parquet, from local disk if mirrored, else from blob storage."""
    blob_name = getattr(blob_obj, "name", str(blob_obj))
    local_path = Path(blob_name)
    if local_path.exists():
        return pd.read_parquet(local_path)

    data = storage_client.fetch_file_content(BlobUtils.market_data_cache_blob(), blob_name)
    return pd.read_parquet(io.BytesIO(data))


def _read_test_option_chain_parquet(storage_client: MarketDataStorageClient, blob_obj) -> pd.DataFrame:
    """Load a single test-mode option-chain parquet, from local disk if mirrored, else from blob storage."""
    blob_name = getattr(blob_obj, "name", str(blob_obj))
    # Locally, test-mode blobs are mirrored under test_mode_data/<blob_name>.
    local_path = Path(TEST_CACHE_DIRNAME) / blob_name
    if local_path.exists():
        return pd.read_parquet(local_path)

    data = storage_client.fetch_file_content(BlobUtils.test_mode_cache_blob(), blob_name)
    return pd.read_parquet(io.BytesIO(data))


def _build_arrival_summary(storage_client: MarketDataStorageClient, blobs: list, read_fn) -> pd.DataFrame:
    """Shared arrival-summary builder used by both the live and test-mode entry points."""
    all_snapshots = []
    for blob_obj in blobs:
        stock = _extract_underlying_symbol(getattr(blob_obj, "name", str(blob_obj)))
        try:
            df = read_fn(storage_client, blob_obj)
        except Exception:
            logger.exception("Failed reading option-chain parquet for %s", stock)
            continue

        if df.empty or "fetched_at" not in df.columns:
            continue

        df["localtimestamp"] = pd.to_datetime(df["fetched_at"]) + IST_OFFSET
        df["date"] = df["localtimestamp"].dt.date
        # Ensure `batch_second` exists in the snapshot; if missing, keep empty
        if "batch_second" not in df.columns:
            df["batch_second"] = pd.NA
        # A single fetch snapshot has one row per option contract; collapse to one row per snapshot.
        snapshots = df.drop_duplicates(subset=["date", "localtimestamp"])[["date", "localtimestamp", "batch_second"]].copy()
        snapshots["stock"] = stock
        blob_name = getattr(blob_obj, "name", str(blob_obj))
        snapshots["blob_name"] = blob_name
        snapshots = snapshots.sort_values("localtimestamp")
        snapshots["record_seq"] = snapshots.groupby("date").cumcount() + 1
        all_snapshots.append(snapshots)

    if not all_snapshots:
        return pd.DataFrame(columns=["date", "record_seq", "stock", "localtimestamp", "diff", "batch_second", "blob_name"])

    result = pd.concat(all_snapshots, ignore_index=True)
    result.sort_values(["date", "stock", "localtimestamp"], inplace=True)
    result["diff"] = result.groupby(["date", "stock"])["localtimestamp"].diff().fillna(pd.Timedelta(0))
    result.sort_values(["date", "localtimestamp"], inplace=True)
    return result[["date", "record_seq", "stock", "localtimestamp", "diff", "batch_second", "blob_name"]].reset_index(drop=True)


def generate_stock_arrival_summary(
    output_csv: Optional[str] = None,
    last_recent_days: Optional[int] = None,
    save_data_local: Optional[str] = None,
    orig_data_save: bool = False,
) -> pd.DataFrame:
    """Build a per-day, per-stock arrival-time summary from cached option-chain data.

    For every stock's option-chain parquet, takes every distinct `fetched_at`
    snapshot per calendar day (not just the first), tags it with its
    per-stock round number (`record_seq`: 1st snapshot, 2nd snapshot, ...),
    then sorts all stocks together (per day, chronologically) so you see
    every stock's 1st record, then every stock's 2nd record, and so on, along
    with the time gap (`diff`) since that same stock's previous record that day.

    Returns a DataFrame with columns: date, record_seq, stock, localtimestamp, diff.
    """
    storage_client = MarketDataStorageClient()
    blobs = _gather_option_chain_blobs(storage_client)
    logger.info("Found %d option-chain parquet files", len(blobs))

    result = _build_arrival_summary(storage_client, blobs, _read_option_chain_parquet)

    # Optionally save the latest parquet (per stock) as CSV locally
    if save_data_local and not result.empty and orig_data_save:
        out_dir = Path(save_data_local)
        out_dir.mkdir(parents=True, exist_ok=True)
        # pick the latest snapshot per stock
        latest_idx = result.groupby("stock")["localtimestamp"].idxmax()
        latest_rows = result.loc[latest_idx]
        for _, row in latest_rows.iterrows():
            blob_name = row.get("blob_name")
            stock = row.get("stock")
            ts = row.get("localtimestamp")
            if not blob_name:
                continue
            try:
                data = storage_client.fetch_file_content(BlobUtils.market_data_cache_blob(), blob_name)
                df_blob = pd.read_parquet(io.BytesIO(data))
                safe_stock = re.sub(r"[^A-Za-z0-9_.-]", "_", stock)
                fname = f"{safe_stock}_{ts.strftime('%Y%m%dT%H%M%S')}.csv"
                df_blob.to_csv(out_dir / fname, index=False)
            except Exception:
                logger.exception("Failed saving parquet blob '%s' for %s", blob_name, stock)

    # If requested, keep only the most recent `last_recent_days` of data
    if last_recent_days and last_recent_days > 0 and not result.empty:
        date_series = pd.to_datetime(result["date"])
        max_dt = date_series.max()
        cutoff = max_dt - pd.Timedelta(days=last_recent_days - 1)
        result = result[date_series >= cutoff.normalize()].reset_index(drop=True)

    if output_csv:
        result.to_csv(output_csv, index=False)

    return result


def generate_test_stock_arrival_summary(
    output_csv: Optional[str] = None,
    last_recent_days: Optional[int] = None,
    save_data_local: Optional[str] = None,
    orig_data_save: bool = False,
) -> pd.DataFrame:
    """Same as `generate_stock_arrival_summary`, but reads cached option-chain data
    from the test-mode-data blob container instead of market-data-cache.

    Returns a DataFrame with columns: date, record_seq, stock, localtimestamp, diff.
    """
    storage_client = MarketDataStorageClient()
    blobs = _gather_test_option_chain_blobs(storage_client)
    logger.info("Found %d option-chain parquet files in test-mode-data", len(blobs))

    result = _build_arrival_summary(storage_client, blobs, _read_test_option_chain_parquet)

    # Optionally save the latest parquet (per stock) as CSV locally
    if save_data_local and not result.empty and orig_data_save:
        out_dir = Path(save_data_local)
        out_dir.mkdir(parents=True, exist_ok=True)
        latest_idx = result.groupby("stock")["localtimestamp"].idxmax()
        latest_rows = result.loc[latest_idx]
        for _, row in latest_rows.iterrows():
            blob_name = row.get("blob_name")
            stock = row.get("stock")
            ts = row.get("localtimestamp")
            if not blob_name:
                continue
            try:
                data = storage_client.fetch_file_content(BlobUtils.test_mode_cache_blob(), blob_name)
                df_blob = pd.read_parquet(io.BytesIO(data))
                safe_stock = re.sub(r"[^A-Za-z0-9_.-]", "_", stock)
                fname = f"{safe_stock}_{ts.strftime('%Y%m%dT%H%M%S')}.csv"
                df_blob.to_csv(out_dir / fname, index=False)
            except Exception:
                logger.exception("Failed saving test-mode parquet blob '%s' for %s", blob_name, stock)

    # If requested, keep only the most recent `last_recent_days` of data
    if last_recent_days and last_recent_days > 0 and not result.empty:
        date_series = pd.to_datetime(result["date"])
        max_dt = date_series.max()
        cutoff = max_dt - pd.Timedelta(days=last_recent_days - 1)
        result = result[date_series >= cutoff.normalize()].reset_index(drop=True)

    if output_csv:
        result.to_csv(output_csv, index=False)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_on_test_data = True
    last_recent_days = 1
    # Ensure the `sumary` directory exists and store all outputs there
    out_dir = Path("summary")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not run_on_test_data:
        result = generate_stock_arrival_summary(
            output_csv=str(out_dir / "stock_arrival_summary.csv"),
            last_recent_days=last_recent_days,
            save_data_local=str(out_dir),
            orig_data_save=True,
        )
        print(result.to_string(index=False))
    else:
        result = generate_test_stock_arrival_summary(
            output_csv=str(out_dir / "test_stock_arrival_summary.csv"),
            last_recent_days=last_recent_days,
            save_data_local=str(out_dir),
            orig_data_save=True,
        )
        print(result.to_string(index=False))