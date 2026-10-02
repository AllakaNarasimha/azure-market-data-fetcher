import os
import tempfile
import logging
import re
import shutil
from pathlib import Path
import pandas as pd

from utils.blob_utils import BlobUtils
from utils.df_utils import DFUtils
from storage.market_data_storage_client import MarketDataStorageClient

logger = logging.getLogger(__name__)


class AzureBlobMarketIndexer:
    DATE_COLUMN = "timestamp"

    def __init__(self, container_name: str | None = None):
        # Default to the centralized market data container if not provided
        self.container_name = container_name or BlobUtils.market_data_cache_blob()
        # Container client may be None if MARKET_STORAGE_CONNECTION is not configured
        self.container_client = BlobUtils.get_container_client(self.container_name)
        # Storage client used to fetch blob content when a blob path is supplied
        self._storage_client = MarketDataStorageClient()

    def _ensure_local_parquet(self, source: str | object) -> tuple[str, bool]:
        """
        Ensure we have a local parquet file path for `source`.
        If `source` is an existing local path, return it and False (not temporary).
        Otherwise treat `source` as a blob path under `self.container_name`,
        download it to a temp file and return the temp path and True.
        """
        # If caller passed a blob-like object with a 'name' attribute, use it
        if hasattr(source, "name") and isinstance(getattr(source, "name"), str):
            blob_path = source.name
        else:
            blob_path = str(source)

        src_path = Path(blob_path)
        if src_path.exists():
            return str(src_path), False

        # Treat as blob path and fetch from Azure
        try:
            data = self._storage_client.fetch_file_content(self.container_name, blob_path)
        except Exception as e:
            logger.exception("Failed to fetch blob '%s' from container '%s'", blob_path, self.container_name)
            raise

        tmpdir = tempfile.gettempdir()
        local_tmp = os.path.join(tmpdir, os.path.basename(blob_path))
        with open(local_tmp, "wb") as fh:
            fh.write(data)
        return local_tmp, True

    def generate_and_upload_index(self, source: str) -> str:
        local_parquet, is_temp = self._ensure_local_parquet(source)

        try:
            df_parquet = pd.read_parquet(local_parquet)
            if df_parquet.empty:
                raise ValueError("Parquet file is empty")
            df = DFUtils.prepare_ist_candle_dataframe(df_parquet, index_col="fetched_at")

            if not isinstance(df.index, pd.DatetimeIndex):
                raise ValueError("Parquet timestamps could not be converted to a datetime index")

            unique_dates = sorted(df.index.normalize().unique())
            if not unique_dates:
                raise ValueError("No date values found in parquet to index")
            
            min_date_str = unique_dates[0]
            max_date_str = unique_dates[-1]

            symbol_match = re.search(r"(?:^|[\\/])symbol=([^\\/]+)", source['name'])
            symbol = (
                symbol_match.group(1)
                if symbol_match
                else df["symbol"].iloc[0] if "symbol" in df.columns else "UNKNOWN_SYMBOL"
            )

            index_records = []
            for dt in unique_dates:
                df_day = df[df.index.normalize() == dt].copy()
                expiries_str = ""
                if "expiry_date" in df_day.columns:
                    df_day["expiry_date"] = pd.to_datetime(df_day["expiry_date"]).dt.strftime("%Y-%m-%d")
                    expiries = sorted(df_day["expiry_date"].dropna().unique())
                    expiries_str = ",".join(str(e) for e in expiries)

                index_records.append({
                    self.DATE_COLUMN: dt,
                    "expiries": expiries_str,
                    "min_strike": (df_day["strike_price"].min() if "strike_price" in df_day.columns else ""),
                    "max_strike": (df_day["strike_price"].max() if "strike_price" in df_day.columns else ""),
                    "underlying_symbol": symbol,
                })

            df_index = pd.DataFrame(index_records)

            # Build index filename from symbol and date range
            start_formatted = min_date_str.strftime("%Y_%m_%d")
            end_formatted = max_date_str.strftime("%Y_%m_%d")
            index_filename = f"{symbol}_{start_formatted}_to_{end_formatted}_index.csv"

            blob_path = index_filename

            # Write to temp file first
            tmpdir = tempfile.gettempdir()
            temp_local_path = os.path.join(tmpdir, index_filename)
            df_index.to_csv(temp_local_path, index=False)

            # Upload to Azure if available
            if (not BlobUtils.is_running_locally()) and self.container_client:
                blob_client = self.container_client.get_blob_client(blob_path)
                with open(temp_local_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                logger.info("Index successfully uploaded to Azure Blob: %s", blob_path)
                return blob_path

            # Local fallback: move into mirrored local blob root
            local_root = os.getenv(BlobUtils.LOCAL_BLOB_ROOT_ENV, BlobUtils.market_data_cache_blob())
            dst = os.path.join(local_root, self.container_name, index_filename)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(temp_local_path, dst)
            logger.info("Index saved to local path: %s", dst)
            return dst

        finally:
            # Clean up any temporary parquet we created
            if is_temp and os.path.exists(local_parquet):
                try:
                    os.remove(local_parquet)
                except Exception:
                    logger.debug("Failed to remove temp parquet %s", local_parquet)