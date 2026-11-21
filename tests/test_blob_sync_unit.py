from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobPrefix

import brokers.broker_authenticate as ba
import brokers.broker_data_manager as bdm
from storage.market_data_storage_client import MarketDataStorageClient
from utils.blob_utils import BlobSync, BlobUtils


@pytest.fixture
def cloud(monkeypatch):
    """Simulate a live (non-local) run where every container resolves to one mock client."""
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))
    container = MagicMock(name="container")
    monkeypatch.setattr(BlobUtils, "get_container_client", staticmethod(lambda name, conn=None: container))
    return container


def test_get_container_is_cached(cloud, monkeypatch):
    calls = []
    monkeypatch.setattr(BlobUtils, "get_container_client", staticmethod(lambda name, conn=None: calls.append(name) or cloud))
    sync = BlobSync()
    calls.clear()

    assert sync.get_container("c1") is cloud
    assert sync.get_container("c1") is cloud
    assert calls == ["c1"]


def test_read_content_returns_downloaded_bytes(cloud):
    cloud.get_blob_client.return_value.download_blob.return_value.readall.return_value = b"abc"
    assert BlobSync().read_content("c1", "x.csv") == b"abc"
    cloud.get_blob_client.assert_called_with("x.csv")


def test_read_if_exists_returns_none_when_missing(cloud):
    cloud.get_blob_client.return_value.download_blob.side_effect = ResourceNotFoundError("nope")
    assert BlobSync().read_if_exists("c1", "missing.json") is None


def test_read_if_exists_returns_bytes_when_present(cloud):
    cloud.get_blob_client.return_value.download_blob.return_value.readall.return_value = b"{}"
    assert BlobSync().read_if_exists("c1", "present.json") == b"{}"


def test_upload_content_uploads_and_returns_true(cloud):
    assert BlobSync().upload_content("c1", "a/b.json", b"data") is True
    cloud.get_blob_client.return_value.upload_blob.assert_called_once_with(b"data", overwrite=True)


def test_upload_content_works_when_market_cache_container_missing(monkeypatch):
    container = MagicMock(name="container")
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))

    def fake_client(name, conn=None):
        return None if name == BlobUtils.market_data_cache_blob() else container

    monkeypatch.setattr(BlobUtils, "get_container_client", staticmethod(fake_client))
    sync = BlobSync()
    assert sync.is_running_live is False
    assert sync.upload_content("broker-tokens", "zerodha.json", b"{}") is True
    container.get_blob_client.return_value.upload_blob.assert_called_once_with(b"{}", overwrite=True)


def test_upload_content_returns_false_when_disabled(monkeypatch):
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: True))
    assert BlobSync().upload_content("c1", "a.json", b"data") is False


def test_upload_content_returns_false_on_upload_error(cloud):
    cloud.get_blob_client.return_value.upload_blob.side_effect = RuntimeError("boom")
    assert BlobSync().upload_content("c1", "a.json", b"data") is False


def test_list_prefixes_keeps_only_virtual_folders(cloud):
    folder = MagicMock(spec=BlobPrefix)
    folder.name = "candles/"
    blob = MagicMock()
    blob.name = "top.csv"
    cloud.walk_blobs.return_value = [folder, blob]

    assert BlobSync().list_prefixes("c1") == ["candles/"]


def test_read_content_raises_when_no_container(monkeypatch):
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))
    monkeypatch.setattr(BlobUtils, "get_container_client", staticmethod(lambda name, conn=None: None))
    with pytest.raises(RuntimeError):
        BlobSync().read_content("c1", "x")


def test_token_cache_round_trip_uses_blob_sync(cloud, monkeypatch):
    store = {}
    blob_for = lambda name: MagicMock(
        upload_blob=MagicMock(side_effect=lambda data, overwrite: store.__setitem__(name, data)),
        download_blob=MagicMock(return_value=MagicMock(readall=MagicMock(side_effect=lambda: store[name]))),
    )
    cloud.get_blob_client.side_effect = lambda name: blob_for(name)
    monkeypatch.setattr(ba.TokenCache, "__init__", lambda self: None)
    cache = ba.TokenCache()
    cache.is_local = False
    cache._blob_sync = BlobSync()

    cache.save("fyers", {"access_token": "t"})
    assert cache.load("fyers") == {"access_token": "t"}
    assert cache.load("missing") is None


def test_master_file_cache_hit_skips_download(cloud, monkeypatch, tmp_path):
    monkeypatch.setattr(bdm.requests, "get", MagicMock(side_effect=AssertionError("should not download")))
    cloud.get_blob_client.return_value.download_blob.return_value.readall.return_value = b"cached"
    cache = bdm.MasterFileCache(tmp_path)
    cache.is_local = False
    cache._blob_sync = BlobSync()

    assert cache.get("master.csv", "http://example/master.csv") == b"cached"


def test_master_file_cache_miss_downloads_and_uploads(cloud, monkeypatch, tmp_path):
    cloud.get_blob_client.return_value.download_blob.side_effect = ResourceNotFoundError("nope")
    response = MagicMock(content=b"fresh")
    monkeypatch.setattr(bdm.requests, "get", MagicMock(return_value=response))
    cache = bdm.MasterFileCache(tmp_path)
    cache.is_local = False
    cache._blob_sync = BlobSync()

    assert cache.get("master.csv", "http://example/master.csv") == b"fresh"
    cloud.get_blob_client.return_value.upload_blob.assert_called_once_with(b"fresh", overwrite=True)


def test_storage_client_lists_folders_via_blob_sync(cloud):
    folder = MagicMock(spec=BlobPrefix)
    folder.name = "daily-logs/"
    cloud.walk_blobs.return_value = [folder]

    client = MarketDataStorageClient(connection_string="conn")
    assert client.list_folders_in_container("c1") == ["daily-logs/"]


def test_storage_client_fetch_reads_via_blob_sync(cloud):
    cloud.get_blob_client.return_value.download_blob.return_value.readall.return_value = b"csv"
    assert MarketDataStorageClient(connection_string="conn").fetch_file_content("c1", "p/f.csv") == b"csv"


def test_storage_client_upload_raises_when_blob_sync_fails(cloud):
    cloud.get_blob_client.return_value.upload_blob.side_effect = RuntimeError("boom")
    client = MarketDataStorageClient(connection_string="conn")
    with pytest.raises(RuntimeError):
        client.upload_file_content("c1", "p/f.csv", b"data")
