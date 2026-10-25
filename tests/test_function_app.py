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

def setupDependencies(monkeypatch, minutes: int = 1):
    """Arrange and return the loaded `function_app` module with TEST_MODE enabled.

    This mirrors the agent guideline's `setupDependencies()` pattern used for
    repeatable test setup. It sets environment vars and imports/reloads the
    module so the module-level startup window is initialised.
    """
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)

    monkeypatch.setenv("TEST_MODE", "true")
    monkeypatch.setenv("TEST_MODE_MINUTES", str(minutes))

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
    fa = setupDependencies(monkeypatch, minutes=5)
    functions = {function.get_function_name(): function for function in fa.app.get_functions()}

    assert "daily_job" in functions
    assert "option_chain_fetcher" in functions

    daily_binding = functions["daily_job"].get_bindings_dict()["bindings"][0]
    assert daily_binding["schedule"] == "0 0 3 * * 1-5"

    chain_binding = functions["option_chain_fetcher"].get_bindings_dict()["bindings"][0]
    assert chain_binding["schedule"] == fa.DYNAMIC_OPTION_CHAIN_CRON


def test_option_chain_fetcher_batches_symbols(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)
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

