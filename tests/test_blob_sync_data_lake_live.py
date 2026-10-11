"""Opt-in real Azure tests for azure-data-toolkit through BlobSync.

PowerShell (from repository root, toolkit already installed):
    $env:RUN_DELTA_LAKE_BLOB_TESTS = "true"
    $env:TEST_MODE = "true"
    python -m pytest tests/test_blob_sync_data_lake_live.py -v

MARKET_STORAGE_CONNECTION may come from local.settings.json via conftest.py.
TEST_MODE_CACHE_BLOB defaults to test-mode-data and MUST differ from production.
Each test uses market_data_cache/_integration_tests/<UUID>/ in that container.
Data and lock blobs are removed afterward; KEEP_DELTA_TEST_BLOBS=true retains
only that test's artifacts for inspection. No production container is accessed.
These are real network tests, not mocked Azure tests. An explicitly enabled
run fails, rather than skips, if credentials or the toolkit are unavailable.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.live_blob,
    pytest.mark.skipif(
        os.getenv("RUN_DELTA_LAKE_BLOB_TESTS", "").lower() != "true",
        reason="Set RUN_DELTA_LAKE_BLOB_TESTS=true and TEST_MODE=true for real test-container I/O",
    ),
]


@pytest.fixture
def blob_lake(tmp_path, monkeypatch):
    import pandas as pd
    from utils.blob_utils import BlobSync, BlobUtils
    from utils.env_config import EnvConfig

    if not EnvConfig.test_mode():
        pytest.fail("Refusing Azure integration tests without TEST_MODE=true")
    test_container = BlobUtils.test_mode_cache_blob()
    production = BlobUtils.market_data_cache_blob()
    if not test_container or test_container == production:
        pytest.fail("TEST_MODE_CACHE_BLOB must differ from MARKET_DATA_CACHE_BLOB")
    connection = BlobUtils.get_blob_connection_string()
    if not connection:
        pytest.fail("MARKET_STORAGE_CONNECTION is required for the explicitly enabled live run")
    if BlobSync._data_lake_class() is None:
        pytest.fail("Install azure-data-toolkit before running these tests")

    original_overrides = dict(EnvConfig._overrides)
    # Restore overrides even if setup fails before the fixture reaches yield.
    monkeypatch.setattr(EnvConfig, "_overrides", dict(original_overrides))
    EnvConfig.set_override("BLOB_SYNC_ENABLE_DELTA_LAKE", "true")
    get_client = BlobUtils.get_container_client

    def test_client_only(container_name, connection_string=None):
        if container_name != test_container:
            raise AssertionError(f"Test attempted to access non-test container: {container_name}")
        return get_client(container_name, connection_string)

    monkeypatch.setattr(BlobUtils, "get_container_client", test_client_only)

    def make_sync():
        # The ordinary constructor initializes production AND test containers.
        # Suppress that initialization, then enable only the test destination.
        with patch.object(BlobUtils, "is_running_locally", return_value=True):
            sync = BlobSync(connection)
        sync._is_running_locally = False
        sync.is_running_live = True
        sync._test_container_client = sync.get_container(test_container)
        return sync

    run = f"_integration_tests/{uuid4().hex}"
    prefix = f"market_data_cache/{run}"
    local_root = tmp_path / "test_mode_data" / "market_data_cache"
    sync = make_sync()

    @contextmanager
    def open_lake():
        with sync.open_data_lake(test_container, prefix=prefix) as lake:
            assert lake.container_name == test_container
            assert lake.prefix == prefix
            yield lake

    context = type("TestContext", (), {})()
    context.pd = pd
    context.sync = sync
    context.make_sync = make_sync
    context.open = open_lake
    context.run = run
    context.prefix = prefix
    context.container = test_container
    context.local_root = local_root
    try:
        yield context
    finally:
        try:
            if os.getenv("KEEP_DELTA_TEST_BLOBS", "").lower() != "true":
                container = sync.get_container(test_container)
                # Delete only this UUID's data and its cooperative lock blobs.
                for scoped_prefix in (prefix + "/", "_adl_locks/" + prefix + "/"):
                    for blob in sync.list_blobs(test_container, scoped_prefix):
                        assert blob.name.startswith(scoped_prefix)
                        container.get_blob_client(blob.name).delete_blob()
        finally:
            EnvConfig._overrides.clear()
            EnvConfig._overrides.update(original_overrides)


@pytest.mark.parametrize("format,path", [("parquet", "roundtrip.parquet"), ("delta", "delta/roundtrip")])
def test_write_read_overwrite_append_upsert(blob_lake, format, path):
    pd = blob_lake.pd
    with blob_lake.open() as lake:
        lake.write(path, pd.DataFrame({"id": [1, 2], "value": [10, 20]}), format=format)
        assert lake.read(path, format=format).sort_values("id").value.tolist() == [10, 20]
        with pytest.raises(ValueError, match="append requires keys"):
            lake.write(path, pd.DataFrame({"id": [3], "value": [30]}), format=format, mode="append")
        lake.upsert(path, pd.DataFrame({"id": [3], "value": [30]}), keys=["id"], format=format)
        lake.upsert(path, pd.DataFrame({"id": [2, 4, 4], "value": [22, 40, 44]}), keys=["id"], format=format)
        result = lake.read(path, format=format).sort_values("id")
        assert result.id.tolist() == [1, 2, 3, 4]
        assert result.value.tolist() == [10, 22, 30, 44]
        lake.write(path, pd.DataFrame({"id": [5], "value": [50]}), format=format, mode="overwrite")
        assert lake.read(path, format=format).id.tolist() == [5]
        names = [b.name for b in blob_lake.sync.list_blobs(blob_lake.container, blob_lake.prefix + "/")]
        if format == "delta":
            assert any("/delta/roundtrip/_delta_log/" in n for n in names)
        else:
            assert blob_lake.prefix + "/" + path in names


def test_missing_parquet_and_invalid_paths(blob_lake):
    from azure.core.exceptions import ResourceNotFoundError
    with blob_lake.open() as lake:
        assert lake.read("missing.parquet", missing_ok=True).empty
        with pytest.raises(ResourceNotFoundError):
            lake.read("missing.parquet")
        for path in ("/absolute.parquet", "../escape.parquet", "a//b.parquet", ""):
            with pytest.raises(ValueError):
                lake.read(path)


def test_consumer_reconciles_committed_write_without_replay(blob_lake, monkeypatch):
    from utils.storage_contracts import ParquetAccess
    from azure_data_toolkit import WriteOutcomeUnknown
    with blob_lake.open() as lake:
        original = lake.upsert
        calls = []
        def commit_then_timeout(*args, **kwargs):
            calls.append(1)
            original(*args, **kwargs)
            raise WriteOutcomeUnknown('Injected lost response after real commit')
        monkeypatch.setattr(lake, 'upsert', commit_then_timeout)
        access = ParquetAccess(lake)
        data = blob_lake.pd.DataFrame({'id': [1], 'value': [10]})
        access.upsert('consumer/reconcile.parquet', data, keys=['id'])
        assert calls == [1]
        assert access.read('consumer/reconcile.parquet').to_dict('list') == data.to_dict('list')


def test_consumer_revision_and_contract(blob_lake):
    from utils.storage_contracts import ParquetAccess
    with blob_lake.open() as lake:
        access = ParquetAccess(lake)
        path = 'candles/consumer/part-0.parquet'
        data = blob_lake.pd.DataFrame({'timestamp': [1791526500], 'open': [10], 'high': [15], 'low': [9], 'close': [14], 'volume': [100], 'revision': [3]})
        access.upsert(path, data, keys=['timestamp'], revision_col='revision')
        old = data.assign(close=11, revision=2)
        access.upsert(path, old, keys=['timestamp'], revision_col='revision')
        with pytest.raises(ValueError, match='Missing dataset fields'):
            access.upsert(path, old.drop(columns='volume'), keys=['timestamp'], revision_col='revision')
        assert access.read(path).close.tolist() == [14]


def test_filters_time_range_and_sort(blob_lake):
    pd = blob_lake.pd
    with blob_lake.open() as lake:
        df = pd.DataFrame({"id": [3, 1, 2], "symbol": ["B", "A", "A"],
                           "timestamp": pd.to_datetime(["2026-10-09T09:17:00Z", "2026-10-09T09:15:00Z", "2026-10-09T09:16:00Z"])})
        lake.upsert("filters.parquet", df, keys=["id"], sort_by="id")
        assert lake.read("filters.parquet").id.tolist() == [1, 2, 3]
        filtered = lake.read("filters.parquet", filters={"symbol": ["A"]}, timestamp_col="timestamp",
                             start="2026-10-09T09:16:00Z", end="2026-10-09T09:17:00Z")
        assert filtered.id.tolist() == [2]


@pytest.mark.parametrize("format,path", [("parquet", "keys.parquet"), ("delta", "delta/keys")])
def test_null_keys_rejected_without_changing_stored_data(blob_lake, format, path):
    pd = blob_lake.pd
    with blob_lake.open() as lake:
        lake.write(path, pd.DataFrame({"id": [1], "value": [10]}), format=format)
        with pytest.raises(ValueError):
            lake.upsert(path, pd.DataFrame({"id": [None], "value": [99]}), keys=["id"], format=format)
        with pytest.raises(ValueError):
            lake.upsert(path, pd.DataFrame({"id": [2]}), keys=[], format=format)
        assert lake.read(path, format=format).value.tolist() == [10]


def test_partitioned_delta(blob_lake):
    pd = blob_lake.pd
    with blob_lake.open() as lake:
        lake.write("delta/partitioned", pd.DataFrame({"id": [1, 2], "symbol": ["A", "B"], "value": [10, 20]}),
                   format="delta", partition_by=["symbol"])
        lake.upsert("delta/partitioned", pd.DataFrame({"id": [2, 3], "symbol": ["B", "A"], "value": [22, 30]}),
                    format="delta", keys=["id"])
        result = lake.read("delta/partitioned", format="delta", filters={"symbol": "A"})
        assert set(result.id) == {1, 3}
        names = [b.name for b in blob_lake.sync.list_blobs(blob_lake.container, blob_lake.prefix + "/delta/partitioned/")]
        assert any("/symbol=A/" in n for n in names)
        assert any("/symbol=B/" in n for n in names)


@pytest.mark.parametrize("format,path", [("parquet", "parallel.parquet"), ("delta", "delta/parallel")])
def test_parallel_writers_same_destination(blob_lake, format, path):
    pd = blob_lake.pd
    with blob_lake.open() as lake:
        lake.write(path, pd.DataFrame({"id": [-1], "value": [-10]}), format=format)
    def write_one(n):
        with blob_lake.open() as lake:
            lake.upsert(path, pd.DataFrame({"id": [n], "value": [n * 10]}), keys=["id"], format=format)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write_one, range(8)))
    with blob_lake.open() as lake:
        result = lake.read(path, format=format).sort_values("id")
        assert result.id.tolist() == list(range(-1, 8))
        assert result.value.tolist() == [n * 10 for n in range(-1, 8)]


def test_lock_contention(blob_lake):
    from azure_data_toolkit.locking import BlobLock, LockUnavailable
    container = blob_lake.sync.get_container(blob_lake.container)
    lock_path = "_adl_locks/" + blob_lake.prefix + "/contention.parquet.lock"
    holder = BlobLock(container, lock_path, lease_seconds=15, wait_seconds=1)
    contender = BlobLock(container, lock_path, lease_seconds=15, wait_seconds=0.1)
    with holder.acquire() as lost:
        with pytest.raises(LockUnavailable):
            with contender.acquire():
                pytest.fail("Two writers acquired the same lease")
        assert not lost.is_set()
    with contender.acquire() as lost:
        assert not lost.is_set()


def test_blobsync_test_routing_upload_download(blob_lake):
    pd = blob_lake.pd
    local = blob_lake.local_root / "snapshot.parquet"
    local.parent.mkdir(parents=True)
    pd.DataFrame({"id": [1], "optional": [None]}).to_parquet(local, index=False)
    name = blob_lake.run + "/snapshot.parquet"
    blob_lake.sync.upload(str(local), name)
    local.unlink()
    blob_lake.sync.download_if_missing(str(local), name)
    assert pd.read_parquet(local).id.tolist() == [1]
    assert blob_lake.sync.read_content(blob_lake.container, "market_data_cache/" + name)


def test_candle_correction_survives_interleaved_writer(blob_lake, monkeypatch):
    from storage import parquet_cache_manager as cache
    from utils.blob_utils import BlobUtils
    pd = blob_lake.pd
    monkeypatch.setattr(cache, "BlobSync", blob_lake.make_sync)
    monkeypatch.setattr(BlobUtils, "is_running_locally", lambda: False)
    # Scope every candle and registry path to the disposable UUID folder.
    original_name = cache.ParquetCacheManager._blob_name
    monkeypatch.setattr(cache.ParquetCacheManager, "_blob_name",
                        lambda self, *args: blob_lake.run + "/" + original_name(self, *args))
    monkeypatch.setattr(cache.CacheRegistry, "BLOB_NAME", blob_lake.run + "/registry.parquet")
    first = cache.ParquetCacheManager(str(blob_lake.local_root))
    second = cache.ParquetCacheManager(str(blob_lake.local_root))
    when = datetime(2026, 10, 9, 9, 15)
    stamp = int(pd.Timestamp(when, tz="Asia/Kolkata").timestamp())
    def row(ts, close):
        return {"timestamp": ts, "open": 10, "high": 15, "low": 9, "close": close, "volume": 100}
    assert first.save_candles("TEST", [row(stamp, 11)], "1")
    write = first._write_partitions
    def interleave(symbol, interval, df):
        assert second.save_candles(symbol, [row(stamp, 14)], interval)
        return write(symbol, interval, df)
    monkeypatch.setattr(first, "_write_partitions", interleave)
    assert first.save_candles("TEST", [row(stamp + 60, 12)], "1")
    result = first.load_candles("TEST", when, when + timedelta(minutes=1), "1")
    assert [record["close"] for record in result] == [14, 12]


def test_resample_persisted_ohlcv(blob_lake):
    from azure_data_toolkit import resample_ohlcv
    pd = blob_lake.pd
    df = pd.DataFrame({"symbol": ["TEST"] * 5,
                       "timestamp": pd.date_range("2026-10-09 09:15", periods=5, freq="min"),
                       "open": [10, 11, 12, 13, 14], "high": [12, 13, 14, 15, 16],
                       "low": [9, 10, 11, 12, 13], "close": [11, 12, 13, 14, 15], "volume": [100] * 5})
    with blob_lake.open() as lake:
        lake.write("resampling/source.parquet", df)
        bars = resample_ohlcv(lake.read("resampling/source.parquet"), interval="5min", offset="9h15min")
        lake.write("resampling/result.parquet", bars)
        result = lake.read("resampling/result.parquet")
        assert len(result) == 1
        assert result.iloc[0][["open", "high", "low", "close", "volume"]].tolist() == [10, 16, 9, 15, 500]


def test_parallel_independent_partitions(blob_lake):
    pd = blob_lake.pd
    def write_one(n):
        with blob_lake.open() as lake:
            path = f"independent/partition={n}/part-0.parquet"
            lake.write(path, pd.DataFrame({"id": [n]}))
            assert lake.read(path).id.tolist() == [n]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write_one, range(8)))


def test_disabled_toolkit_uses_sdk_in_same_test_container(blob_lake):
    from utils.env_config import EnvConfig
    pd = blob_lake.pd
    EnvConfig.set_override("BLOB_SYNC_ENABLE_DELTA_LAKE", "false")
    path = blob_lake.run + "/disabled.parquet"
    with blob_lake.sync.open_parquet(str(blob_lake.local_root)) as backend:
        backend.upsert(path, pd.DataFrame({"id": [1], "value": [10]}), keys=["id"])
        backend.upsert(path, pd.DataFrame({"id": [1, 2], "value": [11, 20]}), keys=["id"])
        assert backend.read(path).sort_values("id").value.tolist() == [11, 20]
    assert blob_lake.sync.read_content(blob_lake.container, "market_data_cache/" + path)


def test_production_container_access_is_rejected(blob_lake):
    from utils.blob_utils import BlobUtils
    with pytest.raises(AssertionError, match="non-test container"):
        blob_lake.sync.get_container(BlobUtils.market_data_cache_blob())


def test_etag_conflict_preserves_uncoordinated_writer(blob_lake, monkeypatch):
    import io
    from azure.core.exceptions import ResourceModifiedError
    pd = blob_lake.pd
    path = "etag.parquet"
    with blob_lake.open() as lake:
        lake.write(path, pd.DataFrame({"id": [1], "value": [10]}))
        read = lake._read_parquet_blob
        def interfere(full_path, *, missing_ok):
            current = read(full_path, missing_ok=missing_ok)
            # Intentionally bypass the cooperative lease in this test only.
            # The toolkit must reject its stale ETag rather than lose this write.
            out = io.BytesIO()
            pd.DataFrame({"id": [1], "value": [99]}).to_parquet(out, index=False)
            container = blob_lake.sync.get_container(blob_lake.container)
            assert full_path.startswith(blob_lake.prefix + "/")
            container.get_blob_client(full_path).upload_blob(out.getvalue(), overwrite=True)
            return current
        with monkeypatch.context() as scoped:
            scoped.setattr(lake, "_read_parquet_blob", interfere)
            with pytest.raises(ResourceModifiedError):
                lake.upsert(path, pd.DataFrame({"id": [2], "value": [20]}), keys=["id"])
        assert lake.read(path).value.tolist() == [99]
