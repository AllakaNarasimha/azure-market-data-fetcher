# BlobSync shared storage

Deployment and local requirements pin azure-data-toolkit to merged commit
`cdb9b0b541b7c30c88a075a73803c2bbdbe286f8`. Install `requirements.txt` in every
writer. `BLOB_SYNC_APP_ID` optionally identifies the service in toolkit logs.

All remote Parquet uploads now use the toolkit's shared lock when
`BLOB_SYNC_ENABLE_DELTA_LAKE` is enabled (the default). Create-only byte uploads
retain `overwrite=False` under the same lock, using commit checks from this
pinned toolkit. A toolkit failure never triggers an unlocked SDK fallback.
CSV, JSON, and explicitly disabled SDK mode retain their legacy behavior.
Disabled SDK mode is not safe for multiple writers and rejects revision/schema
options rather than pretending to enforce them. Consumers should use read-only
credentials where possible. All writers must use the same canonical paths and
lock protocol; direct SDK/manual writes can bypass coordination.

## Dataset contracts

| Dataset | Business keys | Required types |
| --- | --- | --- |
| Candles | timestamp (inside symbol/interval/month partition) | Numeric OHLCV and timestamp, no nulls |
| Quotes | symbol, source, fetched_at | Strings and a non-null pandas datetime |
| Option chains | symbol, source, expiry_timestamp, fetched_at | Strings, underlying, non-null fetched_at datetime |
| Market-off registry | symbol, interval, start_time, end_time, reason | Strings and non-null datetime bounds |
| Expiries | Full replacement snapshot | Broker columns retained; overwrite semantics |

The contract validates required fields and keys without casting stored data,
dropping broker columns, changing paths or adding new metadata columns. It is
not a complete schema registry. Dynamic broker fields remain compatible. For a
fixed dataset, supply an agreed Arrow schema; toolkit validation checks existing
stored types as well. Strict schema options apply to remote toolkit operations;
the local cache retains its existing serialization behavior.

## Source revisions

```python
cache = ParquetCacheManager(revision_col="source_revision", schema=agreed_schema)
cache.save_candles(symbol, incoming_candles, interval="5")
```

`schema` may be omitted. Every row must include a non-null globally comparable
source revision, and existing partitions must be migrated before enabling it.
The candle timestamp is its key, not a revision. Retrieval time cannot establish
which broker correction is newest. The current broker normalizers do not supply
authoritative correction revisions, so existing scheduler calls intentionally
retain last-writer-wins behavior. Do not claim stale-event protection is enabled
until the upstream revision source and existing-data migration are agreed.
Local revision-aware merges use the same highest-revision rule. For custom
writers use `BlobSync.open_parquet()` and pass `revision_col`/`schema` to upsert.
Equal revisions must carry identical payloads across writers.

## Uncertain writes

On `WriteOutcomeUnknown`, keyed upserts perform one bounded read verification.
Success means every requested key has the requested payload or a greater source
revision. Verification does not prove this invocation committed, and is not a
durable exactly-once receipt. If verification fails or is inconclusive, the
original exception propagates. There is no write replay, broker refetch, or
unlocked fallback. Uncertain overwrites always propagate. Candle save/clean and
registry methods do not turn this exception into a normal boolean/void failure.
Schedulers log failures; operators must investigate unresolved outcomes before
manually retrying, preserving the original snapshot keys and revisions.

Existing quote and option keys already include fetched_at. Retrying a snapshot
must reuse that fetched_at; generating a new one creates a new historical row.
Expiry replacement is not a cross-file transaction with chain data. Coordinated
batch/version metadata, stronger fencing, Azure access policy changes, and an
upstream revision migration remain separate work.

## Verification

```powershell
python -m pytest tests/test_storage_contracts.py tests/test_blob_upload_fallback.py -q
```

The consumer live tests still require the existing explicit opt-in,
`RUN_DELTA_LAKE_BLOB_TESTS=true`, `TEST_MODE=true`, a distinct
`TEST_MODE_CACHE_BLOB=spec-tests`, and `MARKET_STORAGE_CONNECTION`. Supply that
connection string securely in the test process, not in files or shell history.
Do not set `WEBSITE_SITE_NAME` to impersonate deployment: the live fixture
simulates the host and rejects non-test containers itself.

```powershell
python -m pytest tests/test_blob_sync_data_lake_live.py -v --tb=short
```

Using the working Python prompt on Windows, run from this app's repository root:

```powershell
$testRunner = @'
import os, sys, getpass, subprocess
account, container = 'marketdata0916', 'spec-tests'
key = getpass.getpass('Paste storage Key1 (hidden): ').strip()
os.environ.update({
    'TEST_MODE': 'true', 'TEST_MODE_CACHE_BLOB': container,
    'RUN_DELTA_LAKE_BLOB_TESTS': 'true',
    'MARKET_STORAGE_CONNECTION': f'DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net'
})
sys.exit(subprocess.call([sys.executable, '-m', 'pytest',
    'tests/test_blob_sync_data_lake_live.py', '-v', '--tb=short']))
'@
python -c $testRunner
```

This is the app's opt-in consumer suite (19 cases), not the toolkit's
`--run-azure` suite. It includes real commits followed by simulated lost responses
to verify that consumers reconcile intent without replaying writes.

These tests use unique test-container prefixes and clean up only their own data
and locks. Local offline tests do not establish live Azure correctness for this
consumer change. The earlier 12 passing live tests verified the toolkit version,
not these new consumer paths.
