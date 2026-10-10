"""Shared Azure Blob container constants and container-client helper.

Every area of the app that persists to Blob Storage (market data cache,
test-mode cache, broker master files, broker tokens) used to duplicate the
same BlobServiceClient/get_container_client/create_container boilerplate.
Centralizing it here means a blob's container name alone identifies which
part of the app wrote it, and any container name change happens in one spot.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobPrefix, BlobServiceClient, ContainerClient

from utils.cache_utils import is_test_blob_path, test_blob_name
from utils.env_config import EnvConfig

logger = logging.getLogger(__name__)


class BlobUtils:
    BROKER_DATA_BLOB = "broker-master"
    BROKER_TOKEN_BLOB = "broker-tokens"
    LOCAL_BLOB_ROOT_ENV = "LOCAL_BLOB_ROOT"

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


class BlobSync:
    def __init__(self, connection_string: Optional[str] = None):
        self._connection_string = connection_string
        self._containers: dict[str, Optional[ContainerClient]] = {}
        self._is_running_locally = BlobUtils.is_running_locally()
        self.is_running_live = not self._is_running_locally
        self._container_client = None
        self._test_container_client = None
        if self.is_running_live:
            self._container_client = self.get_container(BlobUtils.market_data_cache_blob())
            self._test_container_client = self.get_container(BlobUtils.test_mode_cache_blob())
            if not self._container_client:
                self.is_running_live = False

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

    def download_if_missing(self, local_path: str, blob_name: str, container_name: str | None = None) -> None:
        # Skip when running locally or when the file already exists
        if not self.is_running_live or os.path.exists(local_path):
            return
        try:
            container_client, is_test = self._resolve_container(local_path, container_name, op_name="download_if_missing")
            if not container_client:
                return
            blob_name = self._resolve_blob_name(blob_name, is_test, local_path)
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
