"""Shared Azure Blob container constants and container-client helper.

Every area of the app that persists to Blob Storage (market data cache,
test-mode cache, broker master files, broker tokens) used to duplicate the
same BlobServiceClient/get_container_client/create_container boilerplate.
Centralizing it here means a blob's container name alone identifies which
part of the app wrote it, and any container name change happens in one spot.
"""

from __future__ import annotations

import logging
import io
import os
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobPrefix, BlobServiceClient, ContainerClient

from utils.cache_utils import is_test_blob_path, test_blob_name
from utils.env_config import EnvConfig

logger = logging.getLogger(__name__)


class BlobUtils:
    BROKER_DATA_BLOB = "broker-master"
    BROKER_TOKEN_BLOB = "broker-tokens"
    LOCAL_BLOB_ROOT_ENV = "LOCAL_BLOB_ROOT"
    DELTA_LAKE_ENABLED_ENV = "BLOB_SYNC_ENABLE_DELTA_LAKE"

    @staticmethod
    def market_data_cache_blob() -> str:
        return EnvConfig.market_data_cache_blob()

    @staticmethod
    def test_mode_cache_blob() -> str:
        return EnvConfig.test_mode_cache_blob()
    @staticmethod
    def is_running_locally() -> bool:
        return EnvConfig.website_site_name() is None

    @staticmethod
    def website_site_name() -> Optional[str]:
        return EnvConfig.website_site_name()

    @staticmethod
    def website_slot_name() -> str:
        return EnvConfig.website_slot_name()

    @staticmethod
    def environment() -> str:
        return EnvConfig.environment()


    @staticmethod
    def get_container_client(container_name: str, connection_string: Optional[str] = None) -> Optional[ContainerClient]:
        conn_str = connection_string or EnvConfig.market_storage_connection()
        if not conn_str:
            return None

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service.get_container_client(container_name)
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
        except Exception:
            logger.exception("Failed to create blob container '%s'", container_name)
        return container_client

    @staticmethod
    def get_blob_connection_string() -> Optional[str]:
        """Return the connection string used for blob operations.

        Centralized so callers don't read EnvConfig directly.
        """
        return EnvConfig.market_storage_connection()

    @staticmethod
    def get_connection_string_parts(connection_string: Optional[str] = None) -> dict[str, str]:
        conn_str = connection_string or EnvConfig.market_storage_connection() or ""
        parts: dict[str, str] = {}
        for segment in conn_str.split(";"):
            if "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            parts[key] = value
        return parts


class BlobSync:
    def __init__(self, connection_string: Optional[str] = None):
        self._connection_string = connection_string
        self._containers: dict[str, Optional[ContainerClient]] = {}
        self._is_running_locally = BlobUtils.is_running_locally()
        self.is_running_live = not self._is_running_locally
        self._delta_lake_is_enabled = self._delta_lake_enabled()
        self._data_lake_class_ref = self._data_lake_class()
        self._container_client = None
        self._test_container_client = None
        if self.is_running_live:
            self._container_client = self.get_container(BlobUtils.market_data_cache_blob())
            self._test_container_client = self.get_container(BlobUtils.test_mode_cache_blob())
            if not self._container_client:
                self.is_running_live = False
        logger.info(
            "[BLOB SYNC] initialized: live=%s local=%s delta_enabled=%s delta_available=%s",
            self.is_running_live,
            self._is_running_locally,
            self._delta_lake_is_enabled,
            self._data_lake_class_ref is not None,
        )

    def _client_for(self, local_path: str):
        if is_test_blob_path(local_path, self._is_running_locally):
            return self._test_container_client, True
        return self._container_client, False

    def _ensure_enabled_and_get_container(self, container_name: str, op_name: str = "operation"):
        if not self.is_running_live:
            logger.warning("[BLOB SYNC] %s skipped: BlobSync disabled (local run)", op_name)
            return None

        container_client = self.get_container(container_name)
        if not container_client:
            logger.warning("[BLOB SYNC] No container client available for container '%s'", container_name)
            return None
        return container_client

    def _resolve_container(self, local_path: str, container_name: str | None, op_name: str = "operation"):
        """Resolve which ContainerClient to use and whether this is test-mode.

        Returns a tuple `(container_client, is_test)` or `(None, False)` when
        resolution fails (and logs a warning).
        """
        if container_name:
            container_client = self._ensure_enabled_and_get_container(container_name, op_name=op_name)
            if not container_client:
                return None, False
            return container_client, (container_name == BlobUtils.test_mode_cache_blob())

        container_client, is_test = self._client_for(local_path)
        if not container_client:
            logger.warning("[BLOB SYNC] No container client available for local path %s", local_path)
            return None, False
        return container_client, is_test

    def _resolve_blob_name(self, blob_name: str, is_test: bool, local_path: str) -> str:
        """Map `blob_name` to its test-mode equivalent, logging the remap."""
        if not is_test:
            return blob_name
        mapped_name = test_blob_name(blob_name)
        logger.info("[BLOB SYNC] TEST_MODE detected; mapping %s -> %s/%s for local path %s", blob_name, BlobUtils.test_mode_cache_blob(), mapped_name, local_path)
        return mapped_name

    @staticmethod
    def _is_parquet_path(path_value: str) -> bool:
        return str(path_value).lower().endswith(".parquet")

    @staticmethod
    def _data_lake_class() -> Any | None:
        try:
            module = import_module("azure_data_toolkit")
            return getattr(module, "DataLake", None)
        except Exception:
            return None

    @staticmethod
    def _delta_lake_enabled() -> bool:
        raw = EnvConfig.env(BlobUtils.DELTA_LAKE_ENABLED_ENV, "true") or "true"
        return raw.strip().lower() not in {"0", "false", "no", "off"}

    def _write_parquet_locally(self, local_path: str, dataframe: Any) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(local_path, index=False)

    def _download_parquet_via_data_lake(self, container_name: str, blob_name: str, local_path: str) -> bool:
        try:
            with self.open_data_lake(container_name) as lake:
                dataframe = lake.read(blob_name, missing_ok=True)
            if dataframe is None or (dataframe.empty and len(dataframe.columns) == 0):
                return True
            self._write_parquet_locally(local_path, dataframe)
            logger.info("[BLOB SYNC] Downloaded parquet via DataLake from %s/%s", container_name, blob_name)
            return True
        except Exception:
            logger.exception("[BLOB SYNC] DataLake parquet download failed for %s/%s", container_name, blob_name)
            return False

    def _upload_parquet_via_data_lake(self, container_name: str, blob_name: str, local_path: str) -> bool:
        import pandas as pd

        dataframe = pd.read_parquet(local_path)
        with self.open_data_lake(container_name) as lake:
            # File uploads replace a complete snapshot; keys belong to callers.
            lake.write(blob_name, dataframe, mode="overwrite")
        return True

    def download_if_missing(self, local_path: str, blob_name: str, container_name: str | None = None) -> None:
        # Skip when running locally or when the file already exists
        if not self.is_running_live or os.path.exists(local_path):
            return
        try:
            container_client, is_test = self._resolve_container(local_path, container_name, op_name="download_if_missing")
            if not container_client:
                return
            blob_name = self._resolve_blob_name(blob_name, is_test, local_path)
            resolved_container_name = container_name or container_client.container_name

            if self._is_parquet_path(local_path) and self._is_parquet_path(blob_name) and self._delta_lake_enabled():
                if self._download_parquet_via_data_lake(resolved_container_name, blob_name, local_path):
                    return

            blob = container_client.get_blob_client(blob_name)
            try:
                exists = blob.exists()
            except ResourceNotFoundError:
                self._recreate_container(container_client, blob_name)
                return
            if exists:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(blob.download_blob().readall())
        except Exception:
            logger.exception("[BLOB SYNC] Failed to download %s", blob_name)

    def _recreate_container(self, container_client, blob_name: str) -> bool:
        """Container was missing at upload/download time; recreate it so the next call can succeed."""
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
        except Exception:
            logger.exception("[BLOB SYNC] Failed to recreate missing container for %s", blob_name)
            return False
        logger.warning("[BLOB SYNC] Container was missing for %s; recreated it", blob_name)
        return True

    def upload(self, local_path: str, blob_name: str, container_name: str | None = None) -> None:
        # Skip when running locally
        if not self.is_running_live:
            return
        try:
            container_client, is_test = self._resolve_container(local_path, container_name, op_name="upload")
            if not container_client:
                return
            blob_name = self._resolve_blob_name(blob_name, is_test, local_path)
            resolved_container_name = container_name or container_client.container_name

            if self._is_parquet_path(local_path) and self._is_parquet_path(blob_name) and self._delta_lake_enabled():
                self._upload_parquet_via_data_lake(resolved_container_name, blob_name, local_path)
                return

            blob = container_client.get_blob_client(blob_name)
            try:
                with open(local_path, "rb") as f:
                    blob.upload_blob(f.read(), overwrite=True)
            except ResourceNotFoundError:
                if self._recreate_container(container_client, blob_name):
                    with open(local_path, "rb") as f:
                        blob.upload_blob(f.read(), overwrite=True)
        except Exception:
            logger.exception("[BLOB SYNC] Failed to upload %s", blob_name)
            if self._is_parquet_path(local_path) and self._is_parquet_path(blob_name) and self._delta_lake_enabled():
                raise

    def rename_blob_folder(self, container_name: str, old_folder_path: str, new_folder_path: str) -> bool:
        # Ensure BlobSync is enabled and a container client is available
        container_client = self._ensure_enabled_and_get_container(container_name, op_name="rename_blob_folder")
        if not container_client:
            return False

        old_prefix = old_folder_path if old_folder_path.endswith('/') else f"{old_folder_path}/"
        new_prefix = new_folder_path if new_folder_path.endswith('/') else f"{new_folder_path}/"

        try:
            blobs = list(container_client.list_blobs(name_starts_with=old_prefix))
            if not blobs:
                logger.info("[BLOB SYNC] No blobs found under prefix '%s' in container '%s'", old_prefix, container_name)
                return True

            for blob in blobs:
                relative_path = blob.name[len(old_prefix):]
                dest_path = f"{new_prefix}{relative_path}"
                src_client = container_client.get_blob_client(blob.name)
                dest_client = container_client.get_blob_client(dest_path)
                dest_client.start_copy_from_url(src_client.url)
                src_client.delete_blob()

            logger.info("[BLOB SYNC] Renamed folder '%s' -> '%s' in container '%s'", old_prefix, new_prefix, container_name)
            return True
        except Exception:
            logger.exception("[BLOB SYNC] Failed to rename folder %s -> %s in container %s", old_prefix, new_prefix, container_name)
            return False

    def upload_content(self, container_name: str, blob_name: str, data: bytes, overwrite: bool = True) -> bool:
        """
        Upload raw bytes directly to a blob inside `container_name`.

        Returns True if upload was attempted (and likely succeeded), False
        if BlobSync is disabled or the container client cannot be obtained.
        """
        if self._is_running_locally:
            logger.warning("[BLOB SYNC] upload_content skipped: BlobSync disabled (local run)")
            return False
        container_client = self.get_container(container_name)
        if not container_client:
            logger.warning("[BLOB SYNC] No container client available for container '%s'", container_name)
            return False

        is_test = (container_name == BlobUtils.test_mode_cache_blob())
        blob_name = self._resolve_blob_name(blob_name, is_test, local_path=blob_name)

        try:
            blob = container_client.get_blob_client(blob_name)
            blob.upload_blob(data, overwrite=overwrite)
            logger.info("[BLOB SYNC] Uploaded content to %s/%s", container_name, blob_name)
            return True
        except Exception:
            logger.exception("[BLOB SYNC] Failed to upload content to %s/%s", container_name, blob_name)
            return False

    def upload_content_with_fallback(self, container_name: str, blob_name: str, data: bytes, overwrite: bool = True) -> None:
        """Upload bytes using the storage client's existing two-attempt flow.

        First use upload_content (including its test-container path mapping).
        If it returns False or raises, acquire a fresh container client and
        upload to the original, unmapped blob name. That second destination is
        intentional: it preserves the legacy MarketDataStorageClient fallback.
        Final failures propagate to the caller; other upload_content callers
        retain their existing boolean-return behavior.

        Local filesystem writes remain the storage client's responsibility.
        """
        try:
            if self.upload_content(container_name, blob_name, data, overwrite=overwrite):
                return
        except Exception:
            logger.exception("[BLOB SYNC] upload_content failed; attempting fallback upload to %s/%s", container_name, blob_name)

        try:
            container_client = BlobUtils.get_container_client(container_name, self._connection_string)
            if not container_client:
                raise RuntimeError(f"Upload of '{blob_name}' to container '{container_name}' failed: no container client")
            container_client.get_blob_client(blob_name).upload_blob(data, overwrite=overwrite)
            logger.info("[BLOB SYNC] Uploaded content via fallback to %s/%s", container_name, blob_name)
        except Exception:
            logger.exception("[BLOB SYNC] Fallback upload failed for %s/%s", container_name, blob_name)
            raise

    @contextmanager
    def open_data_lake(self, container_name: str, prefix: str = "") -> Iterator[Any]:
        if not self._delta_lake_enabled():
            raise RuntimeError("Delta Lake integration is disabled by environment")

        data_lake_class = self._data_lake_class()
        if data_lake_class is None:
            raise RuntimeError("azure-data-toolkit is not installed; DataLake integration is unavailable")

        parts = BlobUtils.get_connection_string_parts(self._connection_string)
        account_name = parts.get("AccountName")
        account_key = parts.get("AccountKey")
        if not account_name or not account_key:
            raise RuntimeError("MARKET_STORAGE_CONNECTION must include AccountName and AccountKey for DataLake access")

        with data_lake_class(
            account_name=account_name, container=container_name,
            prefix=prefix, account_key=account_key,
        ) as lake:
            yield lake

    @contextmanager
    def open_parquet(self, local_path: str, container_name: str | None = None) -> Iterator[Any]:
        """Resolve cache routing and select the configured Parquet backend.

        Toolkit errors propagate; they must never trigger an unlocked overwrite.
        SDK mode is selected only when the integration is explicitly disabled.
        """
        client, is_test = self._resolve_container(local_path, container_name, "open_parquet")
        if client is None:
            raise RuntimeError("No storage connection available for Parquet access")
        prefix = "market_data_cache" if is_test else ""
        if self._delta_lake_enabled():
            with self.open_data_lake(client.container_name, prefix=prefix) as lake:
                yield lake
        else:
            yield _ParquetBlobBackend(client, prefix)

    def get_container(self, container_name: str) -> Optional[ContainerClient]:
        if container_name not in self._containers:
            self._containers[container_name] = BlobUtils.get_container_client(container_name, self._connection_string)
        return self._containers[container_name]

    def _require_container(self, container_name: str) -> ContainerClient:
        container_client = self.get_container(container_name)
        if not container_client:
            raise RuntimeError(f"No storage connection; cannot access container '{container_name}'")
        return container_client

    def read_content(self, container_name: str, blob_name: str) -> bytes:
        """Download a blob's bytes. Raises ResourceNotFoundError when the blob is missing."""
        return self._require_container(container_name).get_blob_client(blob_name).download_blob().readall()

    def read_if_exists(self, container_name: str, blob_name: str) -> Optional[bytes]:
        try:
            return self.read_content(container_name, blob_name)
        except ResourceNotFoundError:
            return None

    def list_blobs(self, container_name: str, prefix: str = "") -> list:
        return list(self._require_container(container_name).list_blobs(name_starts_with=prefix or None))

    def list_prefixes(self, container_name: str, prefix: str = "") -> list[str]:
        """Immediate virtual folders under `prefix`."""
        blobs = self._require_container(container_name).walk_blobs(name_starts_with=prefix or None, delimiter="/")
        return [item.name for item in blobs if isinstance(item, BlobPrefix)]


class _ParquetBlobBackend:
    """Legacy SDK backend, used only when toolkit integration is disabled."""

    def __init__(self, container: ContainerClient, prefix: str):
        self._container = container
        self._prefix = prefix

    def _blob(self, path: str):
        name = f"{self._prefix}/{path}" if self._prefix else path
        return self._container.get_blob_client(name)

    def read(self, path: str, *, missing_ok: bool = False):
        import pandas as pd

        try:
            content = self._blob(path).download_blob().readall()
        except ResourceNotFoundError:
            if missing_ok:
                return pd.DataFrame()
            raise
        return pd.read_parquet(io.BytesIO(content))

    def write(self, path: str, data: Any, *, mode: str = "overwrite") -> None:
        if mode != "overwrite":
            raise ValueError("Use upsert for merged writes")
        out = io.BytesIO()
        data.to_parquet(out, index=False)
        self._blob(path).upload_blob(out.getvalue(), overwrite=True)

    def upsert(self, path: str, data: Any, *, keys: list[str]) -> None:
        import pandas as pd

        if data.empty:
            return
        if not keys or any(key not in data.columns for key in keys):
            raise ValueError("Explicit business keys required")
        if data[keys].isna().any().any():
            raise ValueError("Business keys cannot be null")
        current = self.read(path, missing_ok=True)
        merged = pd.concat([current, data], ignore_index=True)
        self.write(path, merged.drop_duplicates(subset=keys, keep="last"))
