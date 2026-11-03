"""Stream a market-data Blob container into a ZIP and send it to Telegram."""

import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED, ZipFile

import pytz

from services.telegram_service import TelegramService
from utils.blob_utils import BlobSync, BlobUtils
from utils.market_times import MarketTimes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackupResult:
    container: str
    blob_count: int
    source_bytes: int
    zip_bytes: int
    telegram_message_id: int


class MarketDataBackupService:
    """Read-only backup of one Blob container; no source blobs are deleted."""

    def __init__(self, container_name: Optional[str] = None, blob_sync: Optional[BlobSync] = None) -> None:
        self.container_name = container_name or BlobUtils.market_data_cache_blob()
        self._blob_sync = blob_sync or BlobSync()

    @staticmethod
    def _safe_blob_name(blob_name: str) -> str:
        """Reject names that could escape the ZIP's intended folder."""
        if (
            not blob_name
            or blob_name.startswith("/")
            or "\\" in blob_name
            or any(part in ("", ".", "..") for part in blob_name.split("/"))
        ):
            raise ValueError(f"Unsafe blob path in container: {blob_name!r}")
        return blob_name

    def delete_source_container(self) -> bool:
        """Delete the backed-up container; call only after `run()` succeeded."""
        return self._blob_sync.delete_container(self.container_name)

    def run(self) -> BackupResult:
        telegram = TelegramService.from_environment()  # Fail before download.
        container_client = self._blob_sync.get_container(self.container_name)
        if not container_client:
            raise RuntimeError(f"No storage connection; cannot back up container '{self.container_name}'")

        now = datetime.now(pytz.timezone(MarketTimes.timezone()))
        with tempfile.TemporaryDirectory(prefix="market-backup-") as tmp:
            zip_path = Path(tmp) / f"{self.container_name}_{now:%Y%m%d_%H%M%S}.zip"
            blob_count = 0
            source_bytes = 0

            logger.info("[Backup] Creating backup for container=%s", self.container_name)

            # Stream each blob straight into the ZIP so the container is never staged uncompressed on disk.
            with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED, compresslevel=6, allowZip64=True) as archive:
                for blob in self._blob_sync.list_blobs(self.container_name):
                    if blob.name.endswith("/"):
                        continue  # Virtual folder marker.

                    entry_name = f"{self.container_name}/{self._safe_blob_name(blob.name)}"
                    downloader = container_client.download_blob(blob.name, max_concurrency=2)
                    with archive.open(entry_name, mode="w", force_zip64=True) as entry:
                        for chunk in downloader.chunks():
                            entry.write(chunk)
                            source_bytes += len(chunk)
                    blob_count += 1

            if blob_count == 0:
                raise RuntimeError(
                    f"No blobs found in container {self.container_name!r}; refusing to send an empty backup"
                )

            zip_bytes = zip_path.stat().st_size
            logger.info("[Backup] ZIP created: blobs=%d source_bytes=%d zip_bytes=%d", blob_count, source_bytes, zip_bytes)

            message_id = telegram.send_document(
                zip_path,
                caption=(
                    f"Market data backup: {self.container_name}\n"
                    f"Date: {now:%Y-%m-%d %H:%M} {now.tzname()}\n"
                    f"Blobs: {blob_count} | ZIP: {zip_bytes:,} bytes"
                ),
            )

            return BackupResult(
                container=self.container_name,
                blob_count=blob_count,
                source_bytes=source_bytes,
                zip_bytes=zip_bytes,
                telegram_message_id=message_id,
            )
