"""Consumer integration regressions; Azure I/O is simulated, never contacted."""
import io
from contextlib import contextmanager
from datetime import datetime
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import pyarrow as pa
from azure_data_toolkit import DataLake, WriteOutcomeUnknown
from azure.core.exceptions import ResourceNotFoundError

from utils.blob_utils import BlobSync, BlobUtils
from utils.storage_contracts import ParquetAccess, contract_for
from storage.parquet_cache_manager import ParquetCacheManager
from storage.market_data_cache import QuoteCacheManager, OptionChainCacheManager


def candle(rev=None, close=11):
    row = dict(timestamp=1791526500, open=10, high=15, low=9, close=close, volume=100)
    if rev is not None:
        row['revision'] = rev
    return pd.DataFrame([row])


@pytest.fixture
def backend(monkeypatch):
    lake = object.__new__(DataLake)
    lake.prefix = ''
    lake.container_name = 'spec-tests'
    lake.read_retries = 0
    lake._container = Mock()
    state = {}
    def get_blob(path):
        blob = Mock()
        def properties():
            if path not in state:
                raise ResourceNotFoundError('missing')
            return SimpleNamespace(etag='1')
        blob.get_blob_properties.side_effect = properties
        blob.download_blob.side_effect = lambda **kw: SimpleNamespace(readall=lambda: state[path])
        blob.upload_blob.side_effect = lambda payload, **kw: state.update({path: payload})
        return blob
    lake._container.get_blob_client.side_effect = get_blob
    @contextmanager
    def acquire():
        yield Event()
    monkeypatch.setattr(lake, '_lock', lambda _: SimpleNamespace(acquire=acquire))
    return lake, state


def test_revision_upsert_roundtrip_and_repeat(backend):
    lake, _ = backend
    access = ParquetAccess(lake)
    path = 'candles/symbol=A/interval=5/year=2026/month=10/part-0.parquet'
    access.upsert(path, candle(3, 14), keys=['timestamp'], revision_col='revision')
    for _ in range(2):
        access.upsert(path, candle(2, 11), keys=['timestamp'], revision_col='revision')
    result = access.read(path)
    assert result.close.tolist() == [14]
    assert len(result) == 1


@pytest.mark.parametrize('change', ['key', 'numeric', 'missing'])
def test_invalid_candle_contract_before_io(change):
    data = candle()
    keys = ['timestamp']
    if change == 'key':
        keys = ['close']
    elif change == 'numeric':
        data['open'] = 'bad'
    else:
        data = data.drop(columns='volume')
    lake = Mock()
    with pytest.raises(ValueError):
        ParquetAccess(lake).upsert('candles/x.parquet', data, keys=keys)
    lake.upsert.assert_not_called()


@pytest.mark.parametrize('outcome', ['committed', 'absent', 'different', 'read_failed', 'superseded'])
def test_ambiguous_keyed_write_verification_without_replay(outcome):
    lake = Mock()
    error = WriteOutcomeUnknown('network failed during commit')
    lake.upsert.side_effect = error
    data = candle(2, 11)
    lake.read.return_value = data.copy()
    if outcome == 'absent':
        lake.read.return_value = pd.DataFrame()
    elif outcome == 'different':
        lake.read.return_value = candle(2, 12)
    elif outcome == 'superseded':
        lake.read.return_value = candle(3, 14)
    elif outcome == 'read_failed':
        lake.read.side_effect = ConnectionError('offline')
    access = ParquetAccess(lake)
    if outcome in ('committed', 'superseded'):
        access.upsert('candles/x.parquet', data, keys=['timestamp'], revision_col='revision')
    else:
        with pytest.raises(WriteOutcomeUnknown) as caught:
            access.upsert('candles/x.parquet', data, keys=['timestamp'], revision_col='revision')
        assert caught.value is error
    lake.upsert.assert_called_once()
    lake.write.assert_not_called()


def test_uncertain_overwrite_not_reconciled_or_retried():
    lake = Mock()
    lake.write.side_effect = WriteOutcomeUnknown('uncertain')
    with pytest.raises(WriteOutcomeUnknown):
        ParquetAccess(lake).write('expiries.parquet', pd.DataFrame({'expiry': [1]}))
    lake.write.assert_called_once()
    lake.read.assert_not_called()


def test_composite_key_verification():
    data = pd.DataFrame({'symbol': ['A'], 'source': ['fyers'], 'fetched_at': [datetime(2026, 10, 9)], 'lp': [10.]})
    assert ParquetAccess._contains(data.copy(), data, ['symbol', 'source', 'fetched_at'], None)


def test_sdk_mode_rejects_revision_and_strict_schema():
    lake = Mock()
    access = ParquetAccess(lake, shared=False)
    for kwargs in ({'revision_col': 'revision'}, {'schema': pa.schema([('id', pa.int64())])}):
        with pytest.raises(ValueError, match='requires toolkit'):
            access.upsert('x', pd.DataFrame({'id': [1], 'revision': [1]}), keys=['id'], **kwargs)
    lake.upsert.assert_not_called()


@pytest.fixture
def sync(monkeypatch):
    monkeypatch.setattr(BlobUtils, 'test_mode_cache_blob', lambda: 'spec-tests')
    sync = BlobSync.__new__(BlobSync)
    sync._is_running_locally = False
    sync.is_running_live = True
    sync._connection_string = 'fake'
    sync._containers = {}
    return sync


@pytest.mark.parametrize('overwrite', [True, False])
def test_raw_parquet_upload_uses_test_mapping_and_shared_lock(sync, monkeypatch, overwrite):
    lake = Mock()
    lake._path.side_effect = lambda p: p
    lost = Event()
    @contextmanager
    def acquire():
        yield lost
    lake._lock = Mock()
    lake._lock.return_value.acquire = acquire
    lake._commit.side_effect = lambda action, *args: action()
    @contextmanager
    def open_lake(container):
        assert container == 'spec-tests'
        yield lake
    monkeypatch.setattr(sync, 'open_data_lake', open_lake)
    container = Mock()
    monkeypatch.setattr(sync, '_require_container', lambda _: container)
    payload = io.BytesIO()
    candle().to_parquet(payload, index=False)
    sync.upload_content_with_fallback('spec-tests', 'candles/x.parquet', payload.getvalue(), overwrite)
    if overwrite:
        assert lake.write.call_args.args[0] == 'market_data_cache/candles/x.parquet'
        container.get_blob_client.assert_not_called()
    else:
        lake._lock.assert_called_once_with('market_data_cache/candles/x.parquet')
        container.get_blob_client.return_value.upload_blob.assert_called_once_with(payload.getvalue(), overwrite=False)


def test_raw_parquet_failure_never_uses_unlocked_fallback(sync, monkeypatch):
    error = WriteOutcomeUnknown('uncertain')
    uploader = Mock(side_effect=error)
    monkeypatch.setattr(sync, '_upload_parquet_content', uploader)
    monkeypatch.setattr(BlobUtils, 'get_container_client', Mock())
    with pytest.raises(WriteOutcomeUnknown) as caught:
        sync.upload_content_with_fallback('spec-tests', 'x.parquet', b'bytes')
    assert caught.value is error
    BlobUtils.get_container_client.assert_not_called()


def test_candle_cache_propagates_uncertain_outcome(tmp_path, monkeypatch):
    monkeypatch.setattr(BlobUtils, 'is_running_locally', lambda: True)
    cache = ParquetCacheManager(str(tmp_path))
    cache._write_partitions = Mock(side_effect=WriteOutcomeUnknown('uncertain'))
    with pytest.raises(WriteOutcomeUnknown):
        cache.clean_and_save_candles('A', candle().to_dict('records'))


@pytest.mark.parametrize('kind', ['quote', 'option'])
def test_cache_managers_keep_payload_columns_and_keys(tmp_path, monkeypatch, backend, kind):
    lake, _ = backend
    monkeypatch.setattr(BlobUtils, 'is_running_locally', lambda: True)
    cls = QuoteCacheManager if kind == 'quote' else OptionChainCacheManager
    cache = cls(tmp_path)
    @contextmanager
    def open_parquet(*args):
        yield ParquetAccess(lake)
    cache._blob_sync.open_parquet = open_parquet
    monkeypatch.setattr(BlobUtils, 'is_running_locally', lambda: False)
    when = datetime(2026, 10, 9, 10)
    for _ in range(2):
        if kind == 'quote':
            cache.save('A', {'lp': 10, 'extra': 123}, 'fyers', when)
        else:
            cache.save_fyers_response('A', {'data': {'optionsChain': [{'symbol': 'ACE', 'ltp': 10, 'extra': 123}], 'expiryData': [{'expiry': '1'}]}}, fetched_at=when)
    result = cache.load('A', when.date())
    assert len(result) == 1
    assert result.extra.tolist() == [123]
    if kind == 'option':
        assert cache.load_expiries('A').expiry.tolist() == ['1']


@pytest.mark.parametrize('is_test', [False, True])
def test_open_parquet_preserves_routing_and_applies_contracts(sync, monkeypatch, is_test):
    client = SimpleNamespace(container_name='spec-tests' if is_test else 'market-data-cache')
    monkeypatch.setattr(sync, '_resolve_container', lambda *args: (client, is_test))
    lake = Mock()
    calls = []
    @contextmanager
    def open_lake(container, prefix):
        calls.append((container, prefix))
        yield lake
    monkeypatch.setattr(sync, 'open_data_lake', open_lake)
    with sync.open_parquet('cache') as access:
        assert isinstance(access, ParquetAccess)
        access.upsert('candles/x.parquet', candle(), keys=['timestamp'])
    assert calls == [(client.container_name, 'market_data_cache' if is_test else '')]
    assert lake.upsert.call_args.kwargs == {'keys': ['timestamp']}


def test_toolkit_constructor_enforces_keyed_append(sync, monkeypatch):
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=SimpleNamespace())
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(sync, '_data_lake_class', lambda: factory)
    monkeypatch.setattr(BlobUtils, 'get_connection_string_parts', lambda _: {'AccountName': 'fake', 'AccountKey': 'not-a-real-key'})
    with sync.open_data_lake('spec-tests', prefix='market_data_cache'):
        pass
    assert factory.call_args.kwargs['require_append_keys'] is True
    assert factory.call_args.kwargs['prefix'] == 'market_data_cache'


def test_candle_cache_remote_revision_options_and_incoming_only(tmp_path, monkeypatch, backend):
    lake, _ = backend
    monkeypatch.setattr(BlobUtils, 'is_running_locally', lambda: True)
    cache = ParquetCacheManager(str(tmp_path), revision_col='revision')
    monkeypatch.setattr(BlobUtils, 'is_running_locally', lambda: False)
    @contextmanager
    def open_parquet(*args):
        yield ParquetAccess(lake)
    cache._blob_sync.open_parquet = open_parquet
    cache._merge_with_existing_data = Mock(side_effect=AssertionError('remote premerge is unsafe'))
    assert cache.save_candles('A', candle(3, 14).to_dict('records'))
    assert cache.save_candles('A', candle(2, 11).to_dict('records'))
    path = cache._blob_name('A', '5', 2026, 10)
    assert lake.read(path).close.tolist() == [14]
    cache._merge_with_existing_data.assert_not_called()
