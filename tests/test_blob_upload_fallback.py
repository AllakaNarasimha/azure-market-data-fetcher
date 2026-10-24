"""Upload compatibility checks with mocked Azure clients (no live writes)."""

from unittest.mock import Mock, call

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from storage.market_data_storage_client import MarketDataStorageClient
from utils.blob_utils import BlobSync, BlobUtils


@pytest.fixture
def upload_setup(monkeypatch):
    monkeypatch.setattr(BlobUtils, "is_running_locally", lambda: False)
    monkeypatch.setattr(BlobUtils, "test_mode_cache_blob", lambda: "test-cache")
    primary = Mock()
    fallback = Mock()
    acquire = Mock(return_value=fallback)
    monkeypatch.setattr(BlobUtils, "get_container_client", acquire)

    sync = BlobSync.__new__(BlobSync)
    sync._connection_string = "explicit-connection"
    sync._is_running_locally = False
    sync._containers = {"market-cache": primary, "test-cache": primary}
    client = MarketDataStorageClient.__new__(MarketDataStorageClient)
    client._connection_string = sync._connection_string
    client._blob_sync = sync
    return client, sync, primary, fallback, acquire


@pytest.mark.parametrize("container", ["market-cache", "test-cache"])
@pytest.mark.parametrize("overwrite", [True, False])
@pytest.mark.parametrize("failure", [None, OSError, ResourceNotFoundError, ResourceExistsError])
def test_destinations_payload_and_attempt_order(upload_setup, container, overwrite, failure):
    client, sync, primary, fallback, acquire = upload_setup
    events = Mock()
    events.attach_mock(primary, "primary")
    events.attach_mock(acquire, "acquire")
    events.attach_mock(fallback, "fallback")
    path = "candles/underlying=NSE%3ASBIN-EQ/interval=5/year=2026/month=10/data.parquet"
    data = b"\x00\xffparquet-payload"
    mapped = f"market_data_cache/{path}" if container == "test-cache" else path
    if failure:
        primary.get_blob_client.return_value.upload_blob.side_effect = failure("first attempt failed")

    assert client.upload_file_content(container, path, data, overwrite=overwrite) is None

    expected = [
        call.primary.get_blob_client(mapped),
        call.primary.get_blob_client().upload_blob(data, overwrite=overwrite),
    ]
    if failure:
        expected += [
            call.acquire(container, "explicit-connection"),
            call.fallback.get_blob_client(path),
            call.fallback.get_blob_client().upload_blob(data, overwrite=overwrite),
        ]
    assert events.mock_calls == expected


@pytest.mark.parametrize("primary_result", ["false", "raises"])
def test_fallback_when_primary_unavailable_or_raises(upload_setup, primary_result):
    client, sync, primary, fallback, acquire = upload_setup
    sync.upload_content = Mock(return_value=False)
    if primary_result == "raises":
        sync.upload_content.side_effect = RuntimeError("primary resolution failed")

    client.upload_file_content("test-cache", "index.csv", b"index", overwrite=False)

    acquire.assert_called_once_with("test-cache", "explicit-connection")
    fallback.get_blob_client.assert_called_once_with("index.csv")
    fallback.get_blob_client.return_value.upload_blob.assert_called_once_with(b"index", overwrite=False)


def test_missing_fallback_container_preserves_error(upload_setup):
    client, sync, primary, fallback, acquire = upload_setup
    sync._containers["market-cache"] = None
    acquire.return_value = None

    with pytest.raises(RuntimeError, match="Upload of 'data.parquet' to container 'market-cache' failed: no container client"):
        client.upload_file_content("market-cache", "data.parquet", b"data")


def test_final_failure_propagates_original_exception(upload_setup):
    client, sync, primary, fallback, acquire = upload_setup
    primary.get_blob_client.return_value.upload_blob.side_effect = OSError("primary failed")
    error = ResourceExistsError("overwrite disabled")
    fallback.get_blob_client.return_value.upload_blob.side_effect = error

    with pytest.raises(ResourceExistsError) as raised:
        client.upload_file_content("market-cache", "data.parquet", b"data", overwrite=False)

    assert raised.value is error


def test_existing_upload_content_callers_do_not_gain_fallback(upload_setup):
    client, sync, primary, fallback, acquire = upload_setup
    primary.get_blob_client.return_value.upload_blob.side_effect = OSError("primary failed")

    assert sync.upload_content("market-cache", "data.parquet", b"data") is False
    acquire.assert_not_called()


def test_local_upload_keeps_original_path_and_payload(upload_setup, monkeypatch, tmp_path):
    client, sync, primary, fallback, acquire = upload_setup
    monkeypatch.setattr(BlobUtils, "is_running_locally", lambda: True)
    monkeypatch.setenv(BlobUtils.LOCAL_BLOB_ROOT_ENV, str(tmp_path))
    sync.upload_content_with_fallback = Mock()

    client.upload_file_content("test-cache", "folder/data.parquet", b"local-data", overwrite=False)

    assert (tmp_path / "test-cache" / "folder" / "data.parquet").read_bytes() == b"local-data"
    sync.upload_content_with_fallback.assert_not_called()
    acquire.assert_not_called()
