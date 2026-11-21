"""Stream market data and broker master blobs into one ZIP and send it to Telegram."""

import glob
import logging
import math
import shutil
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED, ZipFile

import pytz

from services.telegram_service import TelegramService
from utils.blob_utils import BlobSync, BlobUtils
from utils.market_times import MarketTimes

logger = logging.getLogger(__name__)

TEMP_DIR_PREFIX = "market-backup-"
FAILED_BACKUP_CONTAINER = "backup-failed"
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5


class NothingToBackUpError(RuntimeError):
    """Every source container is empty; retrying cannot help."""


class TelegramDeliveryError(RuntimeError):
    """A Telegram upload failed after `sent` of `total` parts were already delivered."""

    def __init__(self, sent: int, total: int, cause: Exception) -> None:
        super().__init__(f"{type(cause).__name__}: {cause}")
        self.sent = sent
        self.total = total
        self.cause = cause


@dataclass(frozen=True)
class BackupResult:
    container: str
    blob_count: int
    source_bytes: int
    zip_bytes: int
    telegram_message_ids: tuple[int, ...]
    fallback_blob: Optional[str] = None  # Set when Telegram failed and the ZIP went to FAILED_BACKUP_CONTAINER.


class MarketDataBackupService:
    """Back up market data and broker masters; cleanup targets only market data."""

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

    @staticmethod
    def _clear_stale_temp_dirs() -> None:
        """Remove leftover `market-backup-*` dirs from a prior run that never reached cleanup."""
        pattern = str(Path(tempfile.gettempdir()) / f"{TEMP_DIR_PREFIX}*")
        for stale_dir in glob.glob(pattern):
            shutil.rmtree(stale_dir, ignore_errors=True)

    def delete_source_container(self) -> bool:
        """Delete the backed-up container; call only after `run()` succeeded."""
        return self._blob_sync.delete_container(self.container_name)

    def source_container_exists(self) -> bool:
        return self._blob_sync.container_exists(self.container_name)

    def notify_container_deleted(self, result: BackupResult) -> int:
        now = datetime.now(pytz.timezone(MarketTimes.timezone()))
        if result.fallback_blob:
            outcome = f"Blob container deleted; backup is ONLY in {FAILED_BACKUP_CONTAINER}/{result.fallback_blob} (Telegram failed)"
            delivered = f"Telegram message ID(s) received: {', '.join(str(i) for i in result.telegram_message_ids) or 'none'}"
        else:
            outcome = "Blob container deleted after successful backup"
            delivered = f"Backup message ID(s): {', '.join(str(i) for i in result.telegram_message_ids)}"
        return TelegramService.from_environment().send_message(
            f"{outcome}\n"
            f"Container: {self.container_name}\n"
            f"Date: {now:%Y-%m-%d %H:%M:%S} {now.tzname()}\n"
            f"Total backed-up blobs (market data and broker masters): {result.blob_count}\n"
            f"Total source data: {result.source_bytes:,} bytes\n"
            f"Backup ZIP: {result.zip_bytes:,} bytes\n"
            f"{delivered}"
        )

    def run(self) -> BackupResult:
        telegram = TelegramService.from_environment()  # Fail before download.
        containers = list(dict.fromkeys((self.container_name, BlobUtils.BROKER_DATA_BLOB)))

        # Warm Azure Functions instances share one OS temp dir across invocations; remove any
        # leftover backup dirs from a crashed prior run before adding new data to it.
        self._clear_stale_temp_dirs()

        now = datetime.now(pytz.timezone(MarketTimes.timezone()))
        with tempfile.TemporaryDirectory(prefix=TEMP_DIR_PREFIX) as tmp:
            zip_path = Path(tmp) / f"{self.container_name}_{now:%Y%m%d_%H%M%S}.zip"
            logger.info("[Backup] Creating backup for containers=%s", containers)
            try:
                blob_count, source_bytes = self._retry(
                    "ZIP creation", lambda: self._build_zip(zip_path, containers), no_retry=NothingToBackUpError
                )
            except Exception as zip_error:
                self._send_alert(
                    telegram,
                    f"Backup FAILED: ZIP creation failed ({type(zip_error).__name__}). "
                    f"Source data was kept; container={self.container_name}. See the function logs for details.",
                )
                raise

            zip_bytes = zip_path.stat().st_size
            logger.info("[Backup] ZIP created: blobs=%d source_bytes=%d zip_bytes=%d", blob_count, source_bytes, zip_bytes)

            result = BackupResult(
                container=self.container_name,
                blob_count=blob_count,
                source_bytes=source_bytes,
                zip_bytes=zip_bytes,
                telegram_message_ids=(),
            )
            try:
                message_ids = self._send_to_telegram(telegram, zip_path, containers, now, result)
                return replace(result, telegram_message_ids=message_ids)
            except Exception as telegram_error:
                return self._fall_back_to_failed_container(telegram, zip_path, now, result, telegram_error)

    def _fall_back_to_failed_container(
        self, telegram: TelegramService, zip_path: Path, now: datetime, result: BackupResult, error: Exception
    ) -> BackupResult:
        """Save the full ZIP to FAILED_BACKUP_CONTAINER and alert. Raises if that save fails too."""
        logger.exception("[Backup] Telegram delivery failed; saving ZIP to %s", FAILED_BACKUP_CONTAINER, exc_info=error)
        sent, total = (error.sent, error.total) if isinstance(error, TelegramDeliveryError) else (0, 1)
        reason = type(getattr(error, "cause", error)).__name__
        delivery = (
            f"Telegram received {sent} of {total} part(s) of the backup; the Telegram copy is incomplete "
            f"and must not be used"
            if sent
            else "Nothing was delivered to Telegram"
        )
        try:
            fallback_blob = self._retry("failed-backup upload", lambda: self._save_failed_zip(zip_path, now))
        except Exception as upload_error:
            self._send_alert(
                telegram,
                f"Backup FAILED: Telegram delivery failed ({reason}). {delivery}. "
                f"Saving the full ZIP to {FAILED_BACKUP_CONTAINER} also failed "
                f"({type(upload_error).__name__}). "
                f"Source data was kept; container={self.container_name}. See the function logs for details.",
            )
            raise
        self._send_alert(
            telegram,
            f"Backup delivery failed ({reason}). {delivery}. "
            f"Full ZIP saved to {FAILED_BACKUP_CONTAINER}/{fallback_blob}. "
            f"This is now the ONLY copy of the data: download it, then delete it manually.",
        )
        return replace(result, fallback_blob=fallback_blob)

    def _send_to_telegram(
        self, telegram: TelegramService, zip_path: Path, containers: list[str], now: datetime, result: BackupResult
    ) -> tuple[int, ...]:
        """Send the ZIP (split into parts when over Telegram's limit); returns the message ids."""
        parts = self._split_for_telegram(zip_path, telegram.MAX_DOCUMENT_BYTES)
        if len(parts) > 1:
            logger.info("[Backup] ZIP exceeds Telegram limit; split into %d parts", len(parts))

        message_ids = []
        for index, part_path in enumerate(parts, start=1):
            caption = (
                f"Market data and broker master backup: {', '.join(containers)}\n"
                f"Date: {now:%Y-%m-%d %H:%M} {now.tzname()}\n"
                f"Blobs: {result.blob_count} | ZIP: {result.zip_bytes:,} bytes"
            )
            if len(parts) > 1:
                part_names = "+".join(p.name for p in parts)
                caption += (
                    f"\nPart {index}/{len(parts)} of split archive: {part_path.name}\n\n"
                    f"How to combine after downloading all parts:\n\n"
                    f"Windows (Command Prompt):\n"
                    f"copy /b {part_names} {zip_path.name}\n\n"
                    f"Mac/Linux (Terminal):\n"
                    f"cat {zip_path.name}.part-* > {zip_path.name}\n"
                    f"unzip -t {zip_path.name}"
                )
            try:
                message_ids.append(telegram.send_document(part_path, caption=caption))
            except Exception as exc:
                raise TelegramDeliveryError(len(message_ids), len(parts), exc) from exc
        return tuple(message_ids)

    @staticmethod
    def _retry(label: str, work, no_retry: type[Exception] | tuple = ()):
        """Run `work` up to MAX_ATTEMPTS times; errors of type `no_retry` are raised at once."""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return work()
            except Exception as exc:
                if isinstance(exc, no_retry) or attempt == MAX_ATTEMPTS:
                    raise
                logger.warning(
                    "[Backup] %s attempt %d/%d failed (%r); retrying in %ds",
                    label, attempt, MAX_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS * attempt,
                )
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    def _build_zip(self, zip_path: Path, containers: list[str]) -> tuple[int, int]:
        """Write all containers into `zip_path` (overwritten on each attempt); returns (blob_count, source_bytes)."""
        blob_count = 0
        source_bytes = 0
        container_blobs: dict[str, list] = {}
        for container_name in containers:
            if not self._blob_sync.get_container(container_name):
                raise RuntimeError(f"No storage connection; cannot back up container '{container_name}'")
            container_blobs[container_name] = [
                b for b in self._blob_sync.list_blobs(container_name) if not b.name.endswith("/")
            ]
            logger.info("[Backup] Container=%s blobs=%d", container_name, len(container_blobs[container_name]))
        empty_containers = [name for name, blobs in container_blobs.items() if not blobs]
        if len(empty_containers) == len(containers):
            raise NothingToBackUpError(f"No blobs found in containers {containers!r}; nothing to back up")
        for name in empty_containers:
            logger.warning("[Backup] Container=%s is empty; backing up the remaining containers", name)

        # Stream each blob straight into the ZIP so the container is never staged uncompressed on disk.
        with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED, compresslevel=6, allowZip64=True) as archive:
            for container_name in containers:
                container_client = self._blob_sync.get_container(container_name)
                container_blob_count = 0
                for blob in container_blobs[container_name]:
                    entry_name = f"{container_name}/{self._safe_blob_name(blob.name)}"
                    downloader = container_client.download_blob(blob.name, max_concurrency=2)
                    with archive.open(entry_name, mode="w", force_zip64=True) as entry:
                        for chunk in downloader.chunks():
                            entry.write(chunk)
                            source_bytes += len(chunk)
                    container_blob_count += 1
                blob_count += container_blob_count
                logger.info("[Backup] Archived container=%s blobs=%d", container_name, container_blob_count)
        return blob_count, source_bytes

    def _save_failed_zip(self, zip_path: Path, now: datetime) -> str:
        """Upload the finished ZIP to the failed-backup container; returns the blob name."""
        container = BlobUtils.get_container_client(FAILED_BACKUP_CONTAINER)
        if not container:
            raise RuntimeError(f"Cannot save failed backup: no client for container '{FAILED_BACKUP_CONTAINER}'")
        # One file per day and container, so a same-day retry overwrites instead of adding a second copy.
        blob_name = f"{now:%Y-%m-%d}/{self.container_name}.zip"
        with zip_path.open("rb") as data:
            container.get_blob_client(blob_name).upload_blob(data, overwrite=True, max_concurrency=2)
        logger.error("[Backup] ZIP saved to %s/%s after Telegram failure", FAILED_BACKUP_CONTAINER, blob_name)
        return blob_name

    @staticmethod
    def _send_alert(telegram: TelegramService, text: str) -> None:
        """Best-effort Telegram text alert; a failure here is only logged."""
        try:
            telegram.send_message(text)
        except Exception:
            logger.exception("[Backup] Failure alert could not be sent")

    @staticmethod
    def _split_for_telegram(zip_path: Path, max_bytes: int) -> list[Path]:
        """Split a ZIP into sequential byte-range parts so each upload fits Telegram's document limit."""
        size = zip_path.stat().st_size
        if size <= max_bytes:
            return [zip_path]

        total_parts = math.ceil(size / max_bytes)
        parts: list[Path] = []
        with zip_path.open("rb") as source:
            for index in range(total_parts):
                chunk = source.read(max_bytes)
                part_path = zip_path.with_name(f"{zip_path.name}.part-{index:04d}")
                part_path.write_bytes(chunk)
                parts.append(part_path)
        return parts
