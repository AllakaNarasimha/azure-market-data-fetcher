import os
import logging
import tempfile
from blob_utils import BlobUtils, BlobSync
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobPrefix

logger = logging.getLogger(__name__)

class MarketDataStorageClient:
    """
    Handles all interactions with Azure Blob Storage for market data.
    When running locally, the client will fall back to a "disabled" state where
    fetch operations still require Azure (per your requirement) but uploads
    optionally write to the local filesystem instead of Blob Storage.
    """
    # Environment variable names used to locate the storage connection string.
    CONNECTION_ENV = "MARKET_STORAGE_CONNECTION"
    FALLBACK_CONNECTION_ENV = "StorageConnection"

    def __init__(self, connection_string: str | None = None):
        """
        Initialize with an explicit connection string or fall back to the
        centralized `BlobUtils.get_blob_connection_string()`.

        Note: this class intentionally avoids keeping a BlobServiceClient
        instance. Container clients are acquired via `BlobUtils.get_container_client`
        so the dependency on `azure.storage.blob.BlobServiceClient` is removed.
        """
        # Prefer an explicit connection string, else use centralized getter
        self._connection_string = connection_string or BlobUtils.get_blob_connection_string()
        # BlobSync is used for syncing local cache files with Azure when available
        self._blob_sync = BlobSync()

    def list_folders_in_container(self, container_name: str) -> list:
        """
        Lists virtual folders (prefixes) inside a specific container.
        Example: ['daily-logs/', 'historical-data/', 'processed-reports/']
        """
        try:
            container_client = BlobUtils.get_container_client(container_name, self._connection_string)
            if not container_client:
                raise RuntimeError("No MARKET_STORAGE_CONNECTION; cannot list folders from Azure Blob")
            # walk_blobs with delimiter '/' groups blobs into virtual directories
            blobs = container_client.walk_blobs(delimiter='/')
            
            folders = []
            for blob in blobs:
                if isinstance(blob, BlobPrefix):  # Indicates a virtual folder path
                    folders.append(blob.name)
            return folders
        except Exception as e:
            logger.error(f"Failed to list folders in container '{container_name}': {str(e)}")
            raise

    def list_files_in_subfolder(self, container_name: str, subfolder_path: str, return_blob_objects: bool = False) -> list:
        """
        Lists all files (blobs) inside a specific subfolder path.
        Example subfolder_path: 'daily-logs/2026-09-24/'
        """
        try:
            container_client = BlobUtils.get_container_client(container_name, self._connection_string)
            if not container_client:
                raise RuntimeError("No MARKET_STORAGE_CONNECTION; cannot list files from Azure Blob")
            # Ensure proper prefix structuring
            prefix = subfolder_path if subfolder_path.endswith('/') else f"{subfolder_path}/"
            
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            # Optionally return the raw blob objects (BlobProperties) so callers
            # can operate on metadata or pass objects directly to helpers that
            # accept blob-like inputs. Default behavior remains returning names.
            blobs = [blob for blob in blob_list if not blob.name.endswith('/')]
            if return_blob_objects:
                return blobs
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list files in subfolder '{subfolder_path}': {str(e)}")
            raise

    def list_folders_recursively(self, container_name: str, prefix: str = "") -> list:
        """
        Recursively lists virtual folders (prefixes) inside a container.

        Returns a flat list of folder paths (each ending with '/'), for example:
        ['candles/', 'candles/underlying=NSE%3AAXISBANK-EQ/', 'option_chain/']

        If running without a container client (local mode), this will walk the
        mirrored local blob root to emulate folder structure.
        """
        try:
            container_client = BlobUtils.get_container_client(container_name, self._connection_string)
            folders = []

            # Normalize prefix to end with '/' when provided
            start_prefix = prefix if prefix == "" or prefix.endswith("/") else f"{prefix}/"

            if container_client:
                def _recurse(pfx: str):
                    blobs = container_client.walk_blobs(name_starts_with=pfx, delimiter='/')
                    for blob in blobs:
                        if isinstance(blob, BlobPrefix):
                            folders.append(blob.name)
                            _recurse(blob.name)

                _recurse(start_prefix)
                return folders

            # Local fallback: walk the local blob root directory structure
            local_root = os.getenv(BlobUtils.LOCAL_BLOB_ROOT_ENV, BlobUtils.DEFAULT_LOCAL_BLOB_ROOT)
            base_dir = os.path.join(local_root, container_name, start_prefix)
            if not os.path.isdir(os.path.join(local_root, container_name)):
                return []

            for root, dirs, files in os.walk(os.path.join(local_root, container_name)):
                # Compute the virtual blob-style prefix relative to container root
                rel = os.path.relpath(root, os.path.join(local_root, container_name))
                if rel == '.':
                    rel = ''
                else:
                    rel = rel.replace('\\', '/') + '/'
                if rel and rel not in folders:
                    folders.append(rel)

            # If a start_prefix was provided, filter results to that subtree
            if start_prefix:
                return [f for f in folders if f.startswith(start_prefix)]

            return folders
        except Exception as e:
            logger.error(f"Failed to recursively list folders in container '{container_name}': {str(e)}")
            raise

    def fetch_file_content(self, container_name: str, blob_path: str) -> bytes:
        """Downloads and returns the raw byte content of a specific file."""
        try:
            container_client = BlobUtils.get_container_client(container_name, self._connection_string)
            if not container_client:
                # Enforce requirement: fetching always happens from Azure
                raise RuntimeError("No MARKET_STORAGE_CONNECTION; fetching must be performed from Azure Blob Storage")
            blob_client = container_client.get_blob_client(blob_path)
            return blob_client.download_blob().readall()
        except ResourceNotFoundError:
            logger.error(f"Blob '{blob_path}' not found in container '{container_name}'.")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch file '{blob_path}': {str(e)}")
            raise

    def upload_file_content(self, container_name: str, blob_path: str, data: bytes, overwrite: bool = True):
        """
        Uploads data bytes to a specific path/folder inside the container.
        Virtual subfolders are created automatically if specified in the blob_path.
        """
        try:
            # If running in Azure, prefer using BlobSync.upload_content to centralize
            # upload behavior. BlobSync will use the connection string from
            # BlobUtils.get_blob_connection_string(). If BlobSync is disabled or
            # fails, fall back to direct container upload or local emulator.
            if not BlobUtils.is_running_locally():
                try:
                    uploaded = self._blob_sync.upload_content(container_name, blob_path, data, overwrite=overwrite)
                    if uploaded:
                        logger.info(f"Successfully uploaded data to '%s' in container '%s' via BlobSync.", blob_path, container_name)
                        return
                except Exception:
                    logger.exception("BlobSync.upload_content failed; attempting direct upload")

                # BlobSync didn't upload; try direct container client
                container_client = BlobUtils.get_container_client(container_name, self._connection_string)
                if container_client:
                    blob_client = container_client.get_blob_client(blob_path)
                    blob_client.upload_blob(data, overwrite=overwrite)
                    logger.info(f"Successfully uploaded data to '%s' in container '%s' via direct client.", blob_path, container_name)
                    return

            # Local fallback: write to a local directory mirroring the blob path
            local_root = os.getenv(BlobUtils.LOCAL_BLOB_ROOT_ENV, BlobUtils.DEFAULT_LOCAL_BLOB_ROOT)
            local_path = os.path.join(local_root, container_name, blob_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(data)
            logger.info("Wrote blob data to local path %s (no MARKET_STORAGE_CONNECTION)", local_path)
            return
        except Exception as e:
            logger.error(f"Failed to upload blob to '{blob_path}': {str(e)}")
            raise
        
    def rename_folder(self, container_name: str, old_folder_path: str, new_folder_path: str):
        if not BlobUtils.is_running_locally():
            try:
                renamed = self._blob_sync.rename_blob_folder(container_name, old_folder_path, new_folder_path)
                if renamed:
                    return
            except Exception:
                logger.exception("Failed to rename folder via BlobSync; falling back to direct container operations")

        # Local emulation or BlobSync fallback: move files in local storage root if present
        local_root = os.getenv(BlobUtils.LOCAL_BLOB_ROOT_ENV, BlobUtils.DEFAULT_LOCAL_BLOB_ROOT)
        old_dir = os.path.join(local_root, container_name, old_folder_path)
        new_dir = os.path.join(local_root, container_name, new_folder_path)
        if os.path.exists(old_dir):
            os.makedirs(os.path.dirname(new_dir), exist_ok=True)
            os.rename(old_dir, new_dir)
            logger.info("Renamed local folder %s -> %s", old_dir, new_dir)
        else:
            logger.warning("Local folder %s not found for rename", old_dir)