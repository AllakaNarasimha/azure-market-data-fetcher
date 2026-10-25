import importlib
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pytest
import pytz

import brokers.broker_data_manager as bdm
import schedulers.live_scheduler as live_scheduler_module
from schedulers.scheduler_utils import is_market_open
from schedulers.market_calendar import MarketCalendar
from utils.market_times import MarketTimes
from utils.cache_utils import is_test_blob_path, test_blob_name as build_test_blob_name
from utils.env_config import EnvConfig
from utils.shared_cache import EXPIRIES_CACHE

def setupDependencies(monkeypatch, minutes: int = 1, test_mode: bool = True, symbols: str | None = None):
    """Arrange and return the loaded `function_app` module (TEST_MODE enabled by default).

    This mirrors the agent guideline's `setupDependencies()` pattern used for
    repeatable test setup. It sets environment vars and imports/reloads the
    module so the module-level startup window is initialised.
    """
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)

    monkeypatch.setenv("TEST_MODE", "true" if test_mode else "false")
    monkeypatch.setenv("TEST_MODE_MINUTES", str(minutes))
    if symbols is not None:
        # EnvConfig overrides (loaded from local.settings.json) win over os.environ.
        monkeypatch.setitem(EnvConfig._overrides, "OPTION_CHAIN_SYMBOLS", symbols)

    for mod_name in ("function_app", "app_settings"):
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    function_app = importlib.import_module("function_app")
    importlib.reload(function_app)
    return function_app


def test_test_mode_window_resets_on_reload(monkeypatch):
    # Arrange
    fa = setupDependencies(monkeypatch, minutes=1)

    # Assert: startup window exists and includes 'now'
    ist_now = datetime.now(fa.IST_TZ)
    # Use EnvConfig-backed test-mode window state (keeps tests independent
    # of module-level implementation details).
    assert EnvConfig._TEST_MODE_START is not None
    assert EnvConfig.get_test_mode_expiry() is not None
    # The startup start should be <= now <= expiry
    assert EnvConfig._TEST_MODE_START <= ist_now <= EnvConfig.get_test_mode_expiry()

    # Act: simulate expiry by moving expiry to 1 second after start
    EnvConfig._TEST_MODE_EXPIRY = EnvConfig._TEST_MODE_START + timedelta(seconds=1)
    # Assert: simulated time after expiry returns inactive
    sim_now = EnvConfig._TEST_MODE_START + timedelta(seconds=2)
    assert not EnvConfig.is_test_mode_active(sim_now)

    # Act: simulate host restart (reload module)
    for mod_name in ("function_app", "app_settings"):
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    fa2 = importlib.import_module("function_app")
    importlib.reload(fa2)

    # Assert: after reload the TEST_MODE window is reset into the future
    now2 = datetime.now(fa2.IST_TZ)
    assert EnvConfig._TEST_MODE_START is not None
    assert EnvConfig.get_test_mode_expiry() is not None
    assert now2 <= EnvConfig.get_test_mode_expiry()


def test_test_mode_uses_all_days_schedule(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)

    assert fa.DYNAMIC_OPTION_CHAIN_CRON.split(" ")[-1] == "*"


def test_market_hours_guard_includes_open_close_and_excludes_closed_times():
    timezone = pytz.timezone(MarketTimes.timezone())
    trading_day = datetime(2026, 10, 23)
    before_open = timezone.localize(trading_day.replace(hour=9, minute=14))
    market_open = timezone.localize(trading_day.replace(hour=9, minute=15))
    market_close = timezone.localize(trading_day.replace(hour=16, minute=0))
    after_close = timezone.localize(trading_day.replace(hour=16, minute=1))

    assert not is_market_open(before_open, MarketCalendar.is_holiday)
    assert is_market_open(market_open, MarketCalendar.is_holiday)
    assert is_market_open(market_close, MarketCalendar.is_holiday)
    assert not is_market_open(after_close, MarketCalendar.is_holiday)


def test_live_scheduler_does_not_create_cache_when_market_is_closed(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "false")
    monkeypatch.setattr(EnvConfig, "_overrides", {})
    timezone = pytz.timezone(MarketTimes.timezone())
    closed_time = timezone.localize(datetime(2026, 10, 23, 16, 1))

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return closed_time if tz is not None else closed_time.replace(tzinfo=None)

    monkeypatch.setattr(live_scheduler_module, "datetime", FixedDateTime)
    monkeypatch.setattr(
        live_scheduler_module,
        "OptionChainCacheManager",
        lambda **kwargs: pytest.fail("cache manager must not be created outside market hours"),
    )

    live_scheduler_module.LiveScheduler().run(symbols=["NSE:TEST-EQ"])


def test_daily_timer_runs_on_startup_in_test_mode(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)
    functions = {function.get_function_name(): function for function in fa.app.get_functions()}
    if "daily_job" not in functions:
        pytest.skip("ENABLE_BACKGROUND_TRIGGERS is False; daily_job not registered")
    daily_binding = functions["daily_job"].get_bindings_dict()["bindings"][0]

    assert daily_binding["runOnStartup"] is True


def test_option_chain_test_path_routes_to_test_container(monkeypatch):
    local_path = "/tmp/test_mode_data/market_data_cache/option_chain/underlying=NSE%3ANIFTY50-INDEX/part-0.parquet"
    blob_name = "option_chain/underlying=NSE%3ANIFTY50-INDEX/part-0.parquet"
    # Ensure TEST_MODE is enabled for this check
    monkeypatch.setenv("TEST_MODE", "true")
    assert is_test_blob_path(local_path, is_local=False)
    assert build_test_blob_name(blob_name) == "market_data_cache/option_chain/underlying=NSE%3ANIFTY50-INDEX/part-0.parquet"


def test_test_mode_completion_logged_once(monkeypatch, caplog):
    fa = setupDependencies(monkeypatch, minutes=5)
    expired_at = EnvConfig.get_test_mode_expiry()
    now = expired_at + timedelta(seconds=1)

    with caplog.at_level(logging.INFO):
        assert not EnvConfig.is_test_mode_active(now)
        assert not EnvConfig.is_test_mode_active(now + timedelta(minutes=1))

    completion_logs = [record for record in caplog.records if "TEST_MODE window completed" in record.message]
    assert len(completion_logs) == 1


def test_daily_prefetch_populates_expiries_cache(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)

    # Use a small, controlled symbol list and fake manager to avoid external calls.
    # DailyScheduler reads symbols via EnvConfig, so override it there (not on `fa`).
    monkeypatch.setitem(EnvConfig._overrides, "OPTION_CHAIN_SYMBOLS", "NSE:TEST-EQ")

    class FakeManager:
        def get_expiries(self, instrument):
            return ["2026-09-30"]

        def get_expiry_dates(self, instrument):
            return []

        def get_option_chain(self, *args, **kwargs):
            return []

    fake_instrument = type("Instr", (), {"symbol": "NSE:TEST-EQ"})()

    # Patch resolver and preferred brokers to use our fake objects
    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: fake_instrument))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": FakeManager()}))

    # Clear cache, run daily prefetch, and validate cache populated
    EXPIRIES_CACHE.clear()
    fa.DailyScheduler().run()

    today = datetime.now(fa.IST_TZ).strftime("%Y-%m-%d")
    cache_key = ("fyers", "NSE:TEST-EQ", today)
    assert cache_key in EXPIRIES_CACHE
    assert isinstance(EXPIRIES_CACHE[cache_key], list)


def test_daily_prefetch_logs_warning_when_no_expiries(monkeypatch, caplog):
    fa = setupDependencies(monkeypatch, minutes=5)

    # DailyScheduler reads symbols via EnvConfig, so override it there (not on `fa`).
    monkeypatch.setitem(EnvConfig._overrides, "OPTION_CHAIN_SYMBOLS", "NSE:EMPTY-EQ")

    class EmptyManager:
        def get_expiries(self, instrument):
            return []

        def get_expiry_dates(self, instrument):
            return []

        def get_option_chain(self, *args, **kwargs):
            return []

    fake_instrument = type("Instr", (), {"symbol": "NSE:EMPTY-EQ"})()
    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: fake_instrument))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": EmptyManager()}))

    EXPIRIES_CACHE.clear()
    with caplog.at_level(logging.WARNING):
        fa.DailyScheduler().run()

    warnings = [r for r in caplog.records if "expiries not resolved during pre-market prefetch" in r.message]
    assert len(warnings) >= 1


def test_container_create_is_requested_once_per_process(monkeypatch):
    from azure.core.exceptions import ResourceExistsError
    from utils import blob_utils

    created = []

    class FakeContainer:
        def __init__(self, name):
            self.name = name

        def create_container(self):
            created.append(self.name)
            raise ResourceExistsError("exists")

    class FakeService:
        def get_container_client(self, name):
            return FakeContainer(name)

    monkeypatch.setattr(blob_utils.BlobServiceClient, "from_connection_string", staticmethod(lambda conn: FakeService()))
    monkeypatch.setattr(blob_utils.BlobUtils, "_ensured_containers", set())

    for _ in range(3):
        blob_utils.BlobUtils.get_container_client("cache-a", "conn")
    blob_utils.BlobUtils.get_container_client("cache-b", "conn")

    assert created == ["cache-a", "cache-b"]


def test_container_create_is_retried_after_unexpected_failure(monkeypatch):
    from utils import blob_utils

    attempts = []

    class FakeContainer:
        def create_container(self):
            attempts.append(1)
            raise RuntimeError("transient")

    class FakeService:
        def get_container_client(self, name):
            return FakeContainer()

    monkeypatch.setattr(blob_utils.BlobServiceClient, "from_connection_string", staticmethod(lambda conn: FakeService()))
    monkeypatch.setattr(blob_utils.BlobUtils, "_ensured_containers", set())

    blob_utils.BlobUtils.get_container_client("cache-a", "conn")
    blob_utils.BlobUtils.get_container_client("cache-a", "conn")

    assert len(attempts) == 2


def _fyers_auth_with_counters(monkeypatch, login_error=None):
    import brokers.broker_authenticate as ba

    attempts = []

    def fake_login(self):
        attempts.append(1)
        time.sleep(0.05)
        if login_error:
            raise login_error
        return "auth-code"

    monkeypatch.setattr(ba.FyersTOTPAuthenticator, "_gate", ba._LoginGate("Fyers"))
    monkeypatch.setattr(ba.FyersTOTPAuthenticator, "_login_with_totp", fake_login)
    monkeypatch.setattr(ba.FyersTOTPAuthenticator, "_exchange_code", lambda self, code: "tok")
    monkeypatch.setattr(ba.TokenCache, "__init__", lambda self: None)
    monkeypatch.setattr(ba.TokenCache, "load", lambda self, broker: None)
    monkeypatch.setattr(ba.TokenCache, "save", lambda self, broker, data: None)
    config = type("Cfg", (), {"app_id": "APP"})()
    return ba, attempts, config


def test_failed_fyers_login_is_not_retried_for_every_caller(monkeypatch):
    ba, attempts, config = _fyers_auth_with_counters(monkeypatch, login_error=RuntimeError("429 rate limited"))

    messages = []
    for _ in range(6):
        with pytest.raises(RuntimeError) as exc:
            ba.FyersTOTPAuthenticator(config=config).get_access_token()
        messages.append(str(exc.value))

    assert len(attempts) == 1
    assert "429 rate limited" in messages[0]
    assert all("login skipped" in m and "429 rate limited" in m for m in messages[1:])


def test_concurrent_fyers_callers_share_one_login(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    ba, attempts, config = _fyers_auth_with_counters(monkeypatch)

    with ThreadPoolExecutor(max_workers=5) as pool:
        tokens = list(pool.map(lambda _: ba.FyersTOTPAuthenticator(config=config).get_access_token(), range(5)))

    assert len(attempts) == 1
    assert tokens == ["APP:tok"] * 5


def test_fyers_login_is_retried_after_cooldown(monkeypatch):
    ba, attempts, config = _fyers_auth_with_counters(monkeypatch, login_error=RuntimeError("boom"))
    monkeypatch.setattr(ba.FyersTOTPAuthenticator._gate, "COOLDOWN_SECONDS", 0)

    for _ in range(2):
        with pytest.raises(RuntimeError):
            ba.FyersTOTPAuthenticator(config=config).get_access_token()

    assert len(attempts) == 2


def test_fyers_exchange_reports_missing_app_secret(monkeypatch):
    import brokers.broker_authenticate as ba

    called = []
    monkeypatch.setattr(ba.requests, "post", lambda *a, **k: called.append(1))

    cfg = type("Cfg", (), {"app_id": "ABCDE12345-100", "app_secret": None, "callback_url": "http://x"})()
    with pytest.raises(RuntimeError, match="FYERS_APP"):
        ba.FyersTOTPAuthenticator(config=cfg)._exchange_code("code")
    assert called == []

    cfg.app_secret = "@Microsoft.KeyVault(SecretUri=https://v.vault.azure.net/secrets/s)"
    with pytest.raises(RuntimeError, match="Key Vault reference"):
        ba.FyersTOTPAuthenticator(config=cfg)._exchange_code("code")


def test_fyers_exchange_trims_secret_and_hints_on_hash_mismatch(monkeypatch):
    import hashlib
    import brokers.broker_authenticate as ba

    class BoomSession:
        def __init__(self, **kwargs):
            raise RuntimeError("sdk down")

    sent = {}

    def fake_post(url, json=None, timeout=None):
        sent.update(json)
        return type("Resp", (), {"status_code": 400, "text": '{"code":-5,"message":"invalid app id hash","s":"error"}'})()

    monkeypatch.setattr(ba.fyersModel, "SessionModel", BoomSession)
    monkeypatch.setattr(ba.requests, "post", fake_post)

    cfg = type("Cfg", (), {"app_id": "ABCDE12345-100", "app_secret": "SECRET0001\n", "callback_url": "http://x"})()
    with pytest.raises(RuntimeError) as exc:
        ba.FyersTOTPAuthenticator(config=cfg)._exchange_code("code")

    assert sent["appIdHash"] == hashlib.sha256(b"ABCDE12345-100:SECRET0001").hexdigest()
    assert "app id and app secret do not match" in str(exc.value)
    assert "SECRET0001" not in str(exc.value)


def test_upload_not_found_clears_container_memo_so_it_is_recreated(monkeypatch):
    from unittest.mock import MagicMock
    from azure.core.exceptions import ResourceNotFoundError
    from utils import blob_utils

    created = []

    class FakeContainer:
        def create_container(self):
            created.append(1)

    class FakeService:
        def get_container_client(self, name):
            return FakeContainer()

    monkeypatch.setattr(blob_utils.BlobServiceClient, "from_connection_string", staticmethod(lambda conn: FakeService()))
    monkeypatch.setattr(blob_utils.BlobUtils, "_ensured_containers", set())
    monkeypatch.setattr(blob_utils.BlobUtils, "is_running_locally", staticmethod(lambda: False))

    blob_utils.BlobUtils.get_container_client("broker-tokens", "conn")
    blob_utils.BlobUtils.get_container_client("broker-tokens", "conn")
    assert len(created) == 1

    primary = MagicMock()
    primary.get_blob_client.return_value.upload_blob.side_effect = ResourceNotFoundError("ContainerNotFound")
    sync = blob_utils.BlobSync.__new__(blob_utils.BlobSync)
    sync._connection_string = "conn"
    sync._is_running_locally = False
    sync._containers = {"broker-tokens": primary}

    assert sync.upload_content("broker-tokens", "fyers.json", b"{}") is False

    blob_utils.BlobUtils.get_container_client("broker-tokens", "conn")
    assert len(created) == 2


def test_token_cache_save_uses_fallback_upload_so_missing_container_is_recreated(monkeypatch):
    from unittest.mock import MagicMock
    import brokers.broker_authenticate as ba

    sync = MagicMock()
    monkeypatch.setattr(ba, "BlobSync", lambda *a, **k: sync)
    monkeypatch.setattr(ba.BlobUtils, "is_running_locally", staticmethod(lambda: False))

    ba.TokenCache().save("fyers", {"accessToken": "tok"})

    sync.upload_content_with_fallback.assert_called_once_with(
        ba.BlobUtils.BROKER_TOKEN_BLOB, "fyers.json", b'{"accessToken": "tok"}'
    )


def test_fyers_token_is_saved_when_no_token_blob_exists(monkeypatch):
    ba, attempts, config = _fyers_auth_with_counters(monkeypatch)
    saved = []
    monkeypatch.setattr(ba.TokenCache, "save", lambda self, broker, data: saved.append((broker, data)))

    assert ba.FyersTOTPAuthenticator(config=config).get_access_token() == "APP:tok"

    assert saved == [("fyers", {"accessToken": "tok"})]


def test_gap_detection_reads_registry_once_not_per_slot(tmp_path, monkeypatch):
    import pandas as pd
    from storage.parquet_cache_manager import CacheRegistry, MarketSession, CacheGapDetector

    reads = []
    monkeypatch.setattr(CacheRegistry, "load", lambda self: reads.append(1) or pd.DataFrame(
        columns=["symbol", "interval", "start_time", "end_time", "reason"]
    ))
    registry = CacheRegistry(str(tmp_path))

    start = datetime(2026, 10, 19, 9, 0)
    end = datetime(2026, 10, 23, 16, 0)
    ranges = CacheGapDetector(MarketSession()).get_missing_ranges([], start, end, "1", registry, "NSE:SBIN-EQ")

    assert ranges, "expected missing slots for an empty cache"
    assert len(reads) == 1


def test_gap_detection_skips_slots_covered_by_registry(tmp_path, monkeypatch):
    import pandas as pd
    from storage.parquet_cache_manager import CacheRegistry, MarketSession, CacheGapDetector

    off = pd.DataFrame([{
        "symbol": "NSE:SBIN-EQ", "interval": "1", "reason": "MARKET_HOLIDAY",
        "start_time": pd.Timestamp("2026-10-19 00:00"), "end_time": pd.Timestamp("2026-10-19 23:59"),
    }])
    monkeypatch.setattr(CacheRegistry, "load", lambda self: off)
    registry = CacheRegistry(str(tmp_path))

    ranges = CacheGapDetector(MarketSession()).get_missing_ranges(
        [], datetime(2026, 10, 19, 9, 0), datetime(2026, 10, 19, 16, 0), "1", registry, "NSE:SBIN-EQ"
    )

    assert ranges == []


def test_daily_scheduler_one_failing_symbol_does_not_stop_others(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5, symbols="NSE:A-EQ,NSE:B-EQ,NSE:C-EQ")
    monkeypatch.setitem(EnvConfig._overrides, "WATCHLIST_SYMBOLS", "NSE:A-EQ,NSE:B-EQ,NSE:C-EQ")

    class FakeManager:
        def get_expiries(self, instrument):
            return ["2026-09-30"]

        def get_option_chain(self, *args, **kwargs):
            return {"data": {}}

    def resolve(symbol):
        if symbol == "NSE:B-EQ":
            raise RuntimeError("boom for B")
        return type("Instr", (), {"symbol": symbol})()

    seen = []
    scheduler = fa.DailyScheduler()
    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(resolve))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": FakeManager()}))
    monkeypatch.setattr(
        scheduler, "_fetch_history_if_missing", lambda manager, broker, instrument, *a, **k: seen.append(instrument.symbol) or True
    )
    monkeypatch.setattr(fa.DailyScheduler, "_fetch_option_chain", staticmethod(lambda *a, **k: None))

    scheduler.run()

    assert "NSE:A-EQ" in seen and "NSE:C-EQ" in seen
    assert "NSE:B-EQ" not in seen


def test_daily_scheduler_unexpected_symbol_error_does_not_stop_others(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5, symbols="NSE:A-EQ,NSE:B-EQ,NSE:C-EQ")
    monkeypatch.setitem(EnvConfig._overrides, "WATCHLIST_SYMBOLS", "NSE:A-EQ,NSE:B-EQ,NSE:C-EQ")
    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: type("Instr", (), {"symbol": s})()))

    class FakeManager:
        def get_expiries(self, instrument):
            return ["2026-09-30"]

    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": FakeManager()}))
    scheduler = fa.DailyScheduler()

    synced, chains = [], []

    def history(symbol, ranges, candle_cache):
        if symbol == "NSE:B-EQ":
            raise ValueError("unexpected")
        synced.append(symbol)

    def chain(symbol, chain_cache):
        if symbol == "NSE:A-EQ":
            raise ValueError("unexpected")
        chains.append(symbol)

    monkeypatch.setattr(scheduler, "_sync_history_for_symbol", history)
    monkeypatch.setattr(scheduler, "_sync_option_chain_for_symbol", chain)

    scheduler.run()

    assert synced == ["NSE:A-EQ", "NSE:C-EQ"]
    assert chains == ["NSE:B-EQ", "NSE:C-EQ"]


def test_live_scheduler_one_failing_symbol_does_not_stop_others(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)
    fetched = []

    class FakeManager:
        def get_expiries(self, instrument):
            return ["2026-09-30"]

        def get_option_chain(self, instrument, strikecount=10, expiries=None):
            if instrument.symbol == "NSE:B-EQ":
                raise RuntimeError("broker failed for B")
            fetched.append(instrument.symbol)
            return {"data": {}}

    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: type("Instr", (), {"symbol": s})()))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": FakeManager()}))
    monkeypatch.setattr(bdm.ExpiryResolver, "resolve_range", staticmethod(lambda expiries, weeks, months: ["2026-09-30"]))
    EXPIRIES_CACHE.clear()

    fa.LiveScheduler(max_concurrency=1).run(symbols=["NSE:A-EQ", "NSE:B-EQ", "NSE:C-EQ"])

    assert sorted(fetched) == ["NSE:A-EQ", "NSE:C-EQ"]


def test_registry_log_off_many_writes_once_and_keeps_every_entry(tmp_path, monkeypatch):
    import pandas as pd
    from storage.parquet_cache_manager import CacheRegistry, CacheConstants

    stored = {}
    upserts = []

    class FakeLake:
        def upsert(self, path, df, keys):
            upserts.append(len(df))
            stored["df"] = df

    class FakeCtx:
        def __enter__(self):
            return FakeLake()

        def __exit__(self, *exc):
            return False

    registry = CacheRegistry(str(tmp_path))
    monkeypatch.setattr(CacheRegistry, "load", lambda self: pd.DataFrame(columns=CacheConstants.REGISTRY_COLUMNS))
    monkeypatch.setattr(registry._blob_sync, "open_parquet", lambda *a, **k: FakeCtx())
    monkeypatch.setattr("storage.parquet_cache_manager.BlobUtils.is_running_locally", staticmethod(lambda: False))

    entries = [
        ("NSE:SBIN-EQ", "1", datetime(2026, 10, day, 15, 30), datetime(2026, 10, day, 16, 0), CacheConstants.DATA_GAP)
        for day in range(1, 31)
    ]
    registry.log_off_many(entries)
    registry.log_off_many([])

    assert upserts == [30]
    assert set(stored["df"]["reason"]) == {CacheConstants.DATA_GAP}


def test_save_candles_logs_all_gaps_in_one_registry_update(tmp_path, monkeypatch):
    from storage.parquet_cache_manager import CacheRegistry, ParquetCacheManager

    batches = []
    monkeypatch.setattr(CacheRegistry, "log_off_many", lambda self, entries: batches.append(list(entries)))
    monkeypatch.setattr(CacheRegistry, "log_off", lambda self, *a, **k: pytest.fail("per-gap registry write"))
    monkeypatch.setattr(CacheRegistry, "load", lambda self: __import__("pandas").DataFrame(
        columns=["symbol", "interval", "start_time", "end_time", "reason"]
    ))

    manager = ParquetCacheManager(cache_dir=str(tmp_path))
    start = datetime(2026, 10, 19, 9, 15)
    end = datetime(2026, 10, 23, 16, 0)
    ts = int(datetime(2026, 10, 19, 9, 15).timestamp())
    candle = {"timestamp": ts, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}

    manager.save_candles("NSE:SBIN-EQ", [candle], interval="1", fetch_start=start, fetch_end=end)

    assert len(batches) == 1
    assert len(batches[0]) >= 1


def _daily_scheduler_for_resume(monkeypatch, tmp_path, symbols, chain_symbols="", *, outcome=None):
    """DailyScheduler wired to fakes; returns (module, scheduler, calls). Work happens in tmp_path."""
    monkeypatch.chdir(tmp_path)
    fa = setupDependencies(monkeypatch, minutes=5, symbols=chain_symbols or symbols)
    monkeypatch.setitem(EnvConfig._overrides, "WATCHLIST_SYMBOLS", symbols)
    monkeypatch.setitem(EnvConfig._overrides, "OPTION_CHAIN_SYMBOLS", chain_symbols)
    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: type("Instr", (), {"symbol": s})()))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": object()}))
    scheduler = fa.DailyScheduler(history_days=1)
    calls = []

    def fetch(manager, broker, instrument, cache, start, end):
        calls.append(instrument.symbol)
        return (outcome or {}).get(instrument.symbol, True)

    monkeypatch.setattr(scheduler, "_fetch_history_if_missing", fetch)
    monkeypatch.setattr(fa.DailyScheduler, "_fetch_option_chain", staticmethod(lambda *a, **k: None))
    return fa, scheduler, calls


def test_daily_job_resumes_where_the_budget_ran_out(monkeypatch, tmp_path):
    fa, scheduler, calls = _daily_scheduler_for_resume(monkeypatch, tmp_path, "NSE:A-EQ,NSE:B-EQ,NSE:C-EQ")
    # Budget runs out after one symbol's work has been done.
    out_of_time = iter([False, False, True, True, True, True])
    monkeypatch.setattr(scheduler, "_out_of_time", lambda: next(out_of_time, True))

    assert scheduler.run(time_budget_seconds=60) is False
    first_run = list(dict.fromkeys(calls))
    assert first_run and len(first_run) < 3

    calls.clear()
    monkeypatch.setattr(scheduler, "_out_of_time", lambda: False)
    assert scheduler.run(time_budget_seconds=60) is True
    assert set(first_run).isdisjoint(set(calls))
    assert set(first_run) | set(calls) == {"NSE:A-EQ", "NSE:B-EQ", "NSE:C-EQ"}

    calls.clear()
    assert scheduler.run(time_budget_seconds=60) is True
    assert calls == []


def test_daily_job_completed_day_is_not_rerun_but_a_new_start_is(monkeypatch, tmp_path):
    fa, scheduler, calls = _daily_scheduler_for_resume(monkeypatch, tmp_path, "NSE:A-EQ,NSE:B-EQ")
    assert scheduler.run(time_budget_seconds=60) is True
    assert sorted(set(calls)) == ["NSE:A-EQ", "NSE:B-EQ"]

    calls.clear()
    scheduler.run(time_budget_seconds=60)
    assert calls == []

    # TEST_MODE restart: a new start time means a new progress record, so the job runs again.
    monkeypatch.setattr(EnvConfig, "_TEST_MODE_START", EnvConfig._TEST_MODE_START + timedelta(seconds=7))
    scheduler.run(time_budget_seconds=60)
    assert sorted(set(calls)) == ["NSE:A-EQ", "NSE:B-EQ"]


def test_daily_job_failing_symbol_is_retried_a_limited_number_of_times(monkeypatch, tmp_path):
    fa, scheduler, calls = _daily_scheduler_for_resume(
        monkeypatch, tmp_path, "NSE:A-EQ,NSE:B-EQ", outcome={"NSE:A-EQ": False}
    )

    for _ in range(5):
        scheduler.run(time_budget_seconds=60)

    assert calls.count("NSE:A-EQ") == 3
    assert calls.count("NSE:B-EQ") == 1


def test_daily_job_without_budget_does_not_write_progress(monkeypatch, tmp_path):
    fa, scheduler, calls = _daily_scheduler_for_resume(monkeypatch, tmp_path, "NSE:A-EQ")

    assert scheduler.run() is True

    assert not list(tmp_path.rglob("daily_progress"))


def test_daily_progress_round_trips_through_blob_storage(monkeypatch):
    from azure.core.exceptions import ResourceNotFoundError
    from schedulers import daily_progress as dp

    store = {}

    class FakeBlob:
        def __init__(self, name):
            self.name = name

        def download_blob(self):
            if self.name not in store:
                raise ResourceNotFoundError("missing")
            return type("D", (), {"readall": lambda s: store[self.name]})()

        def upload_blob(self, data, overwrite=False):
            store[self.name] = data

    class FakeContainer:
        def get_blob_client(self, name):
            return FakeBlob(name)

    class FakeSync:
        def get_container(self, name):
            assert name == "cache-container"
            return FakeContainer()

    monkeypatch.setattr(dp.BlobUtils, "is_running_locally", staticmethod(lambda: False))
    monkeypatch.setattr(dp, "BlobSync", FakeSync)

    progress = dp.DailyProgress("2026-10-26", container="cache-container")
    assert progress.pending(["history:A", "history:B"]) == ["history:A", "history:B"]
    progress.record("history:A", True)
    progress.record("history:B", False)

    reloaded = dp.DailyProgress("2026-10-26", container="cache-container")
    assert reloaded.is_finished("history:A")
    assert not reloaded.is_finished("history:B")
    assert reloaded.tasks["history:B"] == 1
    assert "daily_progress/2026-10-26.json" in store


def test_live_scheduler_does_not_cache_empty_expiries_and_logs(monkeypatch, caplog):
    fa = setupDependencies(monkeypatch, minutes=5)

    class EmptyExpiryManager:
        def get_expiries(self, instrument):
            return []

        def get_option_chain(self, *args, **kwargs):
            pytest.fail("no option chain should be requested without expiries")

    instrument = type("Instr", (), {"symbol": "NSE:EMPTY-EQ"})()
    EXPIRIES_CACHE.clear()
    with caplog.at_level(logging.WARNING):
        fa.LiveScheduler()._fetch_option_chain_range(EmptyExpiryManager(), "fyers", instrument, chain_cache=None)

    today = datetime.now().strftime("%Y-%m-%d")
    assert ("fyers", "NSE:EMPTY-EQ", today) not in EXPIRIES_CACHE
    assert any("returned no expiries" in r.message for r in caplog.records)


def test_live_scheduler_fetches_batch_concurrently(monkeypatch):
    """A PARALLEL_BATCH_SIZE (5) symbol batch should be fetched concurrently,
    not one-by-one, so the broker sees ~5 overlapping calls within the same
    second instead of 5 calls spread out sequentially.
    """
    fa = setupDependencies(monkeypatch, minutes=5)

    symbols = [f"NSE:SYM{i}-EQ" for i in range(5)]
    fa.OPTION_CHAIN_SYMBOLS = symbols

    call_starts: list[float] = []
    call_delay = 0.2

    class SlowManager:
        def get_expiries(self, instrument):
            return ["2026-09-30"]

        def get_option_chain(self, instrument, strikecount=10, expiries=None):
            call_starts.append(time.monotonic())
            time.sleep(call_delay)
            return {"data": {}}

    monkeypatch.setattr(bdm.InstrumentResolver, "resolve", staticmethod(lambda s: type("Instr", (), {"symbol": s})()))
    monkeypatch.setattr(bdm.PreferredBrokers, "managers", staticmethod(lambda: {"fyers": SlowManager()}))
    monkeypatch.setattr(bdm.ExpiryResolver, "resolve_range", staticmethod(lambda expiries, weeks, months: ["2026-09-30"]))

    EXPIRIES_CACHE.clear()

    start = time.monotonic()
    fa.LiveScheduler(max_concurrency=5).run(symbols=symbols)
    elapsed = time.monotonic() - start

    assert len(call_starts) == 5
    # Sequential would take ~5 * call_delay; concurrent stays close to one call_delay.
    assert elapsed < call_delay * len(symbols)
    # All 5 calls should have started within the same short window (i.e. "same second"),
    # not staggered call_delay apart from each other.
    assert max(call_starts) - min(call_starts) < call_delay


def test_background_triggers_registered(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5, test_mode=False, symbols="NSE:A-EQ,NSE:B-EQ")
    functions = {function.get_function_name(): function for function in fa.app.get_functions()}

    assert "daily_job" in functions
    assert "option_chain_fetcher" in functions

    daily_binding = functions["daily_job"].get_bindings_dict()["bindings"][0]
    assert daily_binding["schedule"] == "0 */10 4-10 * * 1-5"
    assert daily_binding["runOnStartup"] is False

    chain_binding = functions["option_chain_fetcher"].get_bindings_dict()["bindings"][0]
    assert chain_binding["schedule"] == fa.DYNAMIC_OPTION_CHAIN_CRON
    assert chain_binding["runOnStartup"] is False


def test_test_mode_runs_jobs_on_startup_and_daily_job_resumes(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5, symbols="NSE:A-EQ,NSE:B-EQ")
    functions = {function.get_function_name(): function for function in fa.app.get_functions()}

    chain = functions["option_chain_fetcher"].get_bindings_dict()["bindings"][0]
    assert chain["runOnStartup"] is True
    assert chain["schedule"] == fa.TEST_MODE_ONCE_CRON

    daily = functions["daily_job"].get_bindings_dict()["bindings"][0]
    assert daily["runOnStartup"] is True
    assert daily["schedule"] == "0 */10 * * * *"


def test_test_mode_option_chain_fetcher_runs_every_batch_once(monkeypatch):
    symbols = [f"NSE:SYM{i}-EQ" for i in range(12)]
    fa = setupDependencies(monkeypatch, minutes=5, symbols=",".join(symbols))
    monkeypatch.setattr(fa, "PARALLEL_BATCH_SIZE", 5)

    calls = []

    class RecordingScheduler:
        def run(self, symbols=None, batch_second=None):
            calls.append((list(symbols), batch_second))

    monkeypatch.setattr(fa, "LiveScheduler", RecordingScheduler)

    class FakeTimer:
        past_due = False
        schedule_status = None

    chain_fn = {f.get_function_name(): f for f in fa.app.get_functions()}["option_chain_fetcher"].get_user_function()
    chain_fn(FakeTimer())

    assert [batch_second for _, batch_second in calls] == [0, 1, 2]
    assert [s for batch, _ in calls for s in batch] == symbols


def test_option_chain_fetcher_batches_symbols(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5, test_mode=False, symbols="NSE:A-EQ")
    symbols = [f"NSE:SYM{i}-EQ" for i in range(fa.PARALLEL_BATCH_SIZE * 60)]
    monkeypatch.setattr(fa, "OPTION_CHAIN_SYMBOLS", symbols)

    calls = []

    class RecordingScheduler:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, symbols=None, batch_second=None):
            calls.append((list(symbols), batch_second))

    monkeypatch.setattr(fa, "LiveScheduler", RecordingScheduler)

    class FakeTimer:
        past_due = False
        schedule_status = None

    chain_fn = {f.get_function_name(): f for f in fa.app.get_functions()}["option_chain_fetcher"].get_user_function()
    chain_fn(FakeTimer())

    assert len(calls) == 1
    batch_symbols, batch_second = calls[0]
    assert 0 <= batch_second < 60
    assert 0 < len(batch_symbols) <= fa.PARALLEL_BATCH_SIZE
    assert all(s in symbols for s in batch_symbols)

