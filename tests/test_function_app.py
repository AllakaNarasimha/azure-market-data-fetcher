import importlib
import logging
import os
import sys
from datetime import datetime, timedelta

from cache_utils import is_test_blob_path, test_blob_name as build_test_blob_name

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

    if "function_app" in sys.modules:
        del sys.modules["function_app"]
    import function_app
    importlib.reload(function_app)
    return function_app


def test_test_mode_window_resets_on_reload(monkeypatch):
    # Arrange
    fa = setupDependencies(monkeypatch, minutes=1)

    # Assert: startup window exists and includes 'now'
    ist_now = datetime.now(fa.IST_TZ)
    # Use EnvConfig-backed test-mode window state (keeps tests independent
    # of module-level implementation details).
    assert fa.EnvConfig._TEST_MODE_START is not None
    assert fa.EnvConfig.get_test_mode_expiry() is not None
    # The startup start should be <= now <= expiry
    assert fa.EnvConfig._TEST_MODE_START <= ist_now <= fa.EnvConfig.get_test_mode_expiry()

    # Act: simulate expiry by moving expiry to 1 second after start
    fa.EnvConfig._TEST_MODE_EXPIRY = fa.EnvConfig._TEST_MODE_START + timedelta(seconds=1)
    # Assert: simulated time after expiry returns inactive
    sim_now = fa.EnvConfig._TEST_MODE_START + timedelta(seconds=2)
    assert not fa.EnvConfig.is_test_mode_active(sim_now)

    # Act: simulate host restart (reload module)
    if "function_app" in sys.modules:
        del sys.modules["function_app"]
    import function_app as fa2
    importlib.reload(fa2)

    # Assert: after reload the TEST_MODE window is reset into the future
    now2 = datetime.now(fa2.IST_TZ)
    assert fa2.EnvConfig._TEST_MODE_START is not None
    assert fa2.EnvConfig.get_test_mode_expiry() is not None
    assert now2 <= fa2.EnvConfig.get_test_mode_expiry()


def test_test_mode_uses_all_days_schedule(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)

    assert fa._live_schedule == "0 * * * * *"


def test_daily_timer_runs_on_startup_in_test_mode(monkeypatch):
    fa = setupDependencies(monkeypatch, minutes=5)
    functions = {function.get_function_name(): function for function in fa.app.get_functions()}
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
    expired_at = fa.EnvConfig.get_test_mode_expiry()
    now = expired_at + timedelta(seconds=1)

    with caplog.at_level(logging.INFO):
        assert not fa.EnvConfig.is_test_mode_active(now)
        assert not fa.EnvConfig.is_test_mode_active(now + timedelta(minutes=1))

    completion_logs = [record for record in caplog.records if "TEST_MODE window completed" in record.message]
    assert len(completion_logs) == 1
