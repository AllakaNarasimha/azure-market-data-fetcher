"""Live spec tests against the Azure Blob account in MARKET_STORAGE_CONNECTION.

All blobs are written to a dedicated `spec-tests` container and removed after each test.
Tests are skipped when the connection string is not configured.
"""

import uuid
from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import HttpResponseError

import brokers.broker_authenticate as ba
import brokers.broker_data_manager as bdm
from storage.market_data_storage_client import MarketDataStorageClient
from utils.blob_utils import BlobSync, BlobUtils

SPEC_CONTAINER = "spec-tests"

pytestmark = pytest.mark.live_blob


@pytest.fixture(scope="module")
def live_conn():
    conn = BlobUtils.get_blob_connection_string()
    if not conn:
        pytest.skip("MARKET_STORAGE_CONNECTION not set; live blob spec tests skipped")
    return conn


@pytest.fixture
def live_sync(live_conn, monkeypatch):
    # Force cloud mode so BlobSync is enabled even on a developer machine.
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))
    return BlobSync()


@pytest.fixture
def spec_name():
    return f"spec/{uuid.uuid4().hex}"


@pytest.fixture
def spec_cleanup(live_conn):
    """Delete every blob created in the spec container during the test."""
    container = BlobUtils.get_container_client(SPEC_CONTAINER, live_conn)
    before = {b.name for b in container.list_blobs()}
    yield container
    for blob in container.list_blobs():
        if blob.name not in before:
            container.get_blob_client(blob.name).delete_blob()


def test_get_container_returns_live_client(live_sync):
    assert live_sync.get_container(SPEC_CONTAINER) is not None


def test_upload_then_read_round_trip(live_sync, spec_name, spec_cleanup):
    path = f"{spec_name}/a.txt"
    assert live_sync.upload_content(SPEC_CONTAINER, path, b"hello") is True
    assert live_sync.read_content(SPEC_CONTAINER, path) == b"hello"


def test_overwrite_replaces_content(live_sync, spec_name, spec_cleanup):
    path = f"{spec_name}/b.txt"
    live_sync.upload_content(SPEC_CONTAINER, path, b"first")
    live_sync.upload_content(SPEC_CONTAINER, path, b"second")
    assert live_sync.read_content(SPEC_CONTAINER, path) == b"second"


def test_read_if_exists_returns_none_when_missing(live_sync, spec_name, spec_cleanup):
    assert live_sync.read_if_exists(SPEC_CONTAINER, f"{spec_name}/missing.txt") is None


def test_read_if_exists_returns_bytes_when_present(live_sync, spec_name, spec_cleanup):
    path = f"{spec_name}/present.json"
    live_sync.upload_content(SPEC_CONTAINER, path, b"{}")
    assert live_sync.read_if_exists(SPEC_CONTAINER, path) == b"{}"


def test_list_prefixes_shows_uploaded_folder(live_sync, spec_name, spec_cleanup):
    live_sync.upload_content(SPEC_CONTAINER, f"{spec_name}/f.csv", b"x")
    top_folder = spec_name.split("/")[0] + "/"
    assert top_folder in live_sync.list_prefixes(SPEC_CONTAINER)


def test_upload_content_returns_false_for_invalid_container(live_sync):
    # Uppercase and underscores are invalid container names in Azure Blob Storage.
    assert live_sync.upload_content("Invalid_Name!", "a.json", b"data") is False


def test_token_cache_round_trip(live_sync, spec_cleanup, monkeypatch):
    monkeypatch.setattr(BlobUtils, "BROKER_TOKEN_BLOB", SPEC_CONTAINER)
    broker = f"spec-{uuid.uuid4().hex}"
    cache = ba.TokenCache()

    cache.save(broker, {"access_token": "t"})
    assert cache.load(broker) == {"access_token": "t"}
    assert cache.load(f"missing-{uuid.uuid4().hex}") is None


def test_master_file_cache_miss_then_hit(live_sync, spec_cleanup, monkeypatch, tmp_path):
    monkeypatch.setattr(bdm.MasterFileCache, "CONTAINER", SPEC_CONTAINER)
    cache = bdm.MasterFileCache(tmp_path)
    url = "http://example/master.csv"

    get_mock = MagicMock(return_value=MagicMock(content=b"fresh"))
    monkeypatch.setattr(bdm.requests, "get", get_mock)
    assert cache.get("master.csv", url) == b"fresh"
    assert get_mock.call_count == 1

    monkeypatch.setattr(bdm.requests, "get", MagicMock(side_effect=AssertionError("should not download")))
    assert cache.get("master.csv", url) == b"fresh"


def test_storage_client_upload_fetch_and_list(live_conn, spec_name, spec_cleanup, monkeypatch):
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))
    client = MarketDataStorageClient(connection_string=live_conn)
    path = f"{spec_name}/f.csv"

    client.upload_file_content(SPEC_CONTAINER, path, b"csv-data")
    assert client.fetch_file_content(SPEC_CONTAINER, path) == b"csv-data"
    assert spec_name.split("/")[0] + "/" in client.list_folders_in_container(SPEC_CONTAINER)


def test_storage_client_upload_raises_on_invalid_container(live_conn, monkeypatch):
    monkeypatch.setattr(BlobUtils, "is_running_locally", staticmethod(lambda: False))
    client = MarketDataStorageClient(connection_string=live_conn)
    with pytest.raises(HttpResponseError):
        client.upload_file_content("Invalid_Name!", "p/f.csv", b"data")
