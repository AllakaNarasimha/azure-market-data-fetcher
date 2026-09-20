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

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient

from cache_utils import is_test_blob_path, test_blob_name
from env_config import EnvConfig

logger = logging.getLogger(__name__)


def is_running_locally() -> bool:
    """True when NOT running inside a deployed Azure Function App.

    WEBSITE_INSTANCE_ID isn't reliably forwarded to the Python worker process
    on Linux Consumption, so use WEBSITE_SITE_NAME instead - it's always set
    by the App Service platform for any deployed Web/Function App.
    """
    return EnvConfig.website_site_name() is None


class BlobUtils:
    """Shared container-name constants and container-client helper for all blob areas."""

    # One constant per functional area - container names must be lowercase
    # letters, numbers, and hyphens only (Azure Blob container naming rules).
    MARKET_DATA_CACHE_BLOB = "market-data-cache"
    TEST_MODE_CACHE_BLOB = "test-mode-data"
    BROKER_DATA_BLOB = "broker-master"
    BROKER_TOKEN_BLOB = "broker-tokens"

    @staticmethod
    def get_container_client(container_name: str, connection_string: Optional[str] = None) -> Optional[ContainerClient]:
        """Get a container client for `container_name`, creating the container if missing.

        Returns None when no connection string is available (e.g. MARKET_STORAGE_CONNECTION
        isn't set), so callers can fall back to local/no-op behavior.
        """
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


class BlobSync:
    """Syncs local parquet cache files with Blob Storage.

    Azure's deployed filesystem is read-only (and /tmp is ephemeral), so cached
    candles/quotes/option chains would otherwise be re-fetched on every cold
    start. Reuses MARKET_STORAGE_CONNECTION (already used for broker tokens/
    master files). No-op locally or when that connection string isn't set.
    """

    def __init__(self):
        self._is_local = is_running_locally()
        self.enabled = not self._is_local
        self._container_client = None
        self._test_container_client = None
        if self.enabled:
            self._container_client = BlobUtils.get_container_client(BlobUtils.MARKET_DATA_CACHE_BLOB)
            self._test_container_client = BlobUtils.get_container_client(BlobUtils.TEST_MODE_CACHE_BLOB)
            if not self._container_client:
                self.enabled = False

    def _client_for(self, local_path: str):
        """Test-mode local paths go to the separate test-mode container, else production."""
        if is_test_blob_path(local_path, self._is_local):
            return self._test_container_client, True
        return self._container_client, False

    def download_if_missing(self, local_path: str, blob_name: str) -> None:
        if not self.enabled or os.path.exists(local_path):
            return
        try:
            # TEST_MODE local paths are synced against a separate test-mode
            # container instead of the production market-data-cache container.
            container_client, is_test = self._client_for(local_path)
            if is_test:
                old_blob = blob_name
                blob_name = test_blob_name(blob_name)
                logger.info("[BLOB SYNC] TEST_MODE detected; mapping %s -> %s/%s for local path %s", old_blob, BlobUtils.TEST_MODE_CACHE_BLOB, blob_name, local_path)
            blob = container_client.get_blob_client(blob_name)
            if blob.exists():
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(blob.download_blob().readall())
        except Exception:
            logger.exception("[BLOB SYNC] Failed to download %s", blob_name)

    def upload(self, local_path: str, blob_name: str) -> None:
        if not self.enabled:
            return
        try:
            # TEST_MODE local paths are synced against a separate test-mode
            # container instead of the production market-data-cache container.
            container_client, is_test = self._client_for(local_path)
            if is_test:
                old_blob = blob_name
                blob_name = test_blob_name(blob_name)
                logger.info("[BLOB SYNC] TEST_MODE detected; mapping %s -> %s/%s for local path %s", old_blob, BlobUtils.TEST_MODE_CACHE_BLOB, blob_name, local_path)
            blob = container_client.get_blob_client(blob_name)
            with open(local_path, "rb") as f:
                blob.upload_blob(f.read(), overwrite=True)
        except Exception:
            logger.exception("[BLOB SYNC] Failed to upload %s", blob_name)
