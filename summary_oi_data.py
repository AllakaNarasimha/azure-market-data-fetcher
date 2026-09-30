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

from blob_utils import BlobUtils
from env_config import EnvConfig
from market_data_storage_client import MarketDataStorageClient

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
PART_FILE_PATTERN = "part-0.parquet"
# fetched_at is stored as a naive UTC datetime (datetime.now() on the host); shift to IST for reporting.
IST_OFFSET = pd.Timedelta(hours=5, minutes=30)

def _gather_option_chain_blobs(storage_client: MarketDataStorageClient) -> list:
    """Collect every option-chain `part-0.parquet` blob across all stocks."""
    folders = storage_client.list_folders_in_container(BlobUtils.MARKET_DATA_CACHE_BLOB)
    matches = []
    for folder in folders:
        if OPTION_CHAIN_PREFIX not in folder:
            continue
        try:
            blob_objs = storage_client.list_files_in_subfolder(
                BlobUtils.MARKET_DATA_CACHE_BLOB, folder, return_blob_objects=True
            )
        except Exception:
            logger.exception("Failed listing option-chain blobs under '%s'", folder)
            continue

        for blob_obj in blob_objs:
            if fnmatch.fnmatch(Path(blob_obj.name).name, PART_FILE_PATTERN):
                matches.append(blob_obj)
    return matches


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

    data = storage_client.fetch_file_content(BlobUtils.MARKET_DATA_CACHE_BLOB, blob_name)
    return pd.read_parquet(io.BytesIO(data))


def generate_stock_arrival_summary(output_csv: Optional[str] = None) -> pd.DataFrame:
    """Build a per-day, per-stock arrival-time summary from cached option-chain data.

    For every stock's option-chain parquet, takes every distinct `fetched_at`
    snapshot per calendar day (not just the first), tags it with its
    per-stock round number (`record_seq`: 1st snapshot, 2nd snapshot, ...),
    then sorts all stocks together (per day, chronologically) so you see
    every stock's 1st record, then every stock's 2nd record, and so on, along
    with the time gap (`diff`) from the previously-arrived record that day.

    Returns a DataFrame with columns: date, record_seq, stock, localtimestamp, diff.
    """
    storage_client = MarketDataStorageClient()
    blobs = _gather_option_chain_blobs(storage_client)
    logger.info("Found %d option-chain parquet files", len(blobs))

    all_snapshots = []
    for blob_obj in blobs:
        stock = _extract_underlying_symbol(getattr(blob_obj, "name", str(blob_obj)))
        try:
            df = _read_option_chain_parquet(storage_client, blob_obj)
        except Exception:
            logger.exception("Failed reading option-chain parquet for %s", stock)
            continue

        if df.empty or "fetched_at" not in df.columns:
            continue

        df["localtimestamp"] = pd.to_datetime(df["fetched_at"]) + IST_OFFSET
        df["date"] = df["localtimestamp"].dt.date
        # A single fetch snapshot has one row per option contract; collapse to one row per snapshot.
        snapshots = df.drop_duplicates(subset=["date", "localtimestamp"])[["date", "localtimestamp"]].copy()
        snapshots["stock"] = stock
        snapshots = snapshots.sort_values("localtimestamp")
        snapshots["record_seq"] = snapshots.groupby("date").cumcount() + 1
        all_snapshots.append(snapshots)

    if not all_snapshots:
        return pd.DataFrame(columns=["date", "record_seq", "stock", "localtimestamp", "diff"])

    result = pd.concat(all_snapshots, ignore_index=True)
    result.sort_values(["date", "localtimestamp"], inplace=True)
    result["diff"] = result.groupby("date")["localtimestamp"].diff().fillna(pd.Timedelta(0))
    result = result[["date", "record_seq", "stock", "localtimestamp", "diff"]].reset_index(drop=True)

    if output_csv:
        result.to_csv(output_csv, index=False)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = generate_stock_arrival_summary(output_csv="stock_arrival_summary.csv")
    print(result.to_string(index=False))