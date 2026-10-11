"""Shared Parquet contracts without changing broker payloads or stored paths."""
from dataclasses import dataclass
import logging

import pandas as pd
from azure_data_toolkit import WriteOutcomeUnknown

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetContract:
    keys: tuple[str, ...]
    numeric: tuple[str, ...] = ()
    strings: tuple[str, ...] = ()
    datetimes: tuple[str, ...] = ()

    def validate(self, data):
        required = set(self.keys + self.numeric + self.strings + self.datetimes)
        missing = required - set(data.columns)
        if missing:
            raise ValueError(f'Missing dataset fields: {sorted(missing)}')
        if data[list(self.keys)].isna().any().any():
            raise ValueError('Business keys must not be null')
        for name in self.numeric:
            if not pd.api.types.is_numeric_dtype(data[name]) or data[name].isna().any():
                raise ValueError(f'{name} must be non-null numeric data')
        for name in self.strings:
            if not data[name].map(lambda value: isinstance(value, str)).all():
                raise ValueError(f'{name} must contain strings')
        for name in self.datetimes:
            if not pd.api.types.is_datetime64_any_dtype(data[name]) or data[name].isna().any():
                raise ValueError(f'{name} must contain non-null datetimes')


CANDLES = DatasetContract(('timestamp',), ('timestamp', 'open', 'high', 'low', 'close', 'volume'))
QUOTES = DatasetContract(('symbol', 'source', 'fetched_at'), strings=('symbol', 'source'), datetimes=('fetched_at',))
OPTIONS = DatasetContract(('symbol', 'source', 'expiry_timestamp', 'fetched_at'), strings=('symbol', 'source', 'expiry_timestamp', 'underlying'), datetimes=('fetched_at',))
REGISTRY = DatasetContract(('symbol', 'interval', 'start_time', 'end_time', 'reason'), strings=('symbol', 'interval', 'reason'), datetimes=('start_time', 'end_time'))


def contract_for(path):
    if path.startswith('market_data_cache/'):
        path = path[len('market_data_cache/'):]
    if path.startswith('candles/'):
        return CANDLES
    if path.startswith('quotes/'):
        return QUOTES
    if path.startswith('option_chain/') and not path.endswith('/expiries.parquet'):
        return OPTIONS
    if path == 'market_off_registry.parquet':
        return REGISTRY
    return None


class ParquetAccess:
    """Validate before I/O; reconcile uncertain keyed writes, never replay them.

    Verification establishes that the requested rows are present (or superseded
    by greater source revisions), not that this invocation performed the commit.
    Overwrite errors propagate because replacement is not a keyed operation.
    """
    def __init__(self, backend, *, shared=True):
        self.backend = backend
        self.shared = shared

    def read(self, path, **kwargs):
        return self.backend.read(path, **kwargs)

    def write(self, path, data, *, mode='overwrite', schema=None):
        contract = contract_for(path)
        if contract:
            contract.validate(data)
        if mode != 'overwrite':
            raise ValueError('Use keyed upsert for appends')
        kwargs = {'mode': mode}
        if schema is not None:
            if not self.shared:
                raise ValueError('Strict schema requires toolkit backend')
            kwargs['schema'] = schema
        self.backend.write(path, data, **kwargs)

    def upsert(self, path, data, *, keys, revision_col=None, schema=None):
        contract = contract_for(path)
        if contract:
            contract.validate(data)
            if tuple(keys) != contract.keys:
                raise ValueError(f'Dataset requires business keys {contract.keys}')
        if not keys or any(key not in data for key in keys) or data[list(keys)].isna().any().any():
            raise ValueError('Explicit non-null business keys required')
        kwargs = {'keys': list(keys)}
        for name, value in [('revision_col', revision_col), ('schema', schema)]:
            if value is not None:
                if not self.shared:
                    raise ValueError(f'{name} requires toolkit backend')
                kwargs[name] = value
        try:
            self.backend.upsert(path, data, **kwargs)
        except WriteOutcomeUnknown:
            try:
                current = self.backend.read(path, missing_ok=False)
                satisfied = self._contains(current, data, keys, revision_col)
            except Exception:
                satisfied = False
            if not satisfied:
                logger.error('[BLOB SYNC] Uncertain keyed write unresolved path=%s; no retry or SDK fallback', path)
                raise
            logger.warning('[BLOB SYNC] Uncertain keyed write verified path=%s rows=%s', path, len(data))

    @staticmethod
    def _contains(current, incoming, keys, revision_col):
        from azure_data_toolkit import merge_frames
        if current.empty or not current[list(keys)].duplicated().eq(False).all():
            return False
        expected = merge_frames(pd.DataFrame(), incoming, keys, revision_col=revision_col)
        stored = current.set_index(list(keys), drop=False)
        for _, row in expected.iterrows():
            key = tuple(row[key] for key in keys)
            key = key[0] if len(keys) == 1 else key
            if key not in stored.index:
                return False
            actual = stored.loc[key]
            if revision_col and actual[revision_col] > row[revision_col]:
                continue
            for column, value in row.items():
                if column not in actual:
                    return False
                found = actual[column]
                if pd.isna(value) and pd.isna(found):
                    continue
                if pd.isna(value) or pd.isna(found) or found != value:
                    return False
        return True
