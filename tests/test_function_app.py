import importlib
import os
import sys
from datetime import datetime, timedelta

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
    assert fa._TEST_MODE_START is not None
    assert fa._TEST_MODE_EXPIRY is not None
    # The startup start should be <= now <= expiry
    assert fa._TEST_MODE_START <= ist_now <= fa._TEST_MODE_EXPIRY

    # Act: simulate expiry by moving expiry to 1 second after start
    fa._TEST_MODE_EXPIRY = fa._TEST_MODE_START + timedelta(seconds=1)
    # Assert: simulated time after expiry returns inactive
    sim_now = fa._TEST_MODE_START + timedelta(seconds=2)
    assert not fa._is_test_mode_active(sim_now)

    # Act: simulate host restart (reload module)
    if "function_app" in sys.modules:
        del sys.modules["function_app"]
    import function_app as fa2
    importlib.reload(fa2)

    # Assert: after reload the TEST_MODE window is reset into the future
    now2 = datetime.now(fa2.IST_TZ)
    assert fa2._TEST_MODE_START is not None
    assert fa2._TEST_MODE_EXPIRY is not None
    assert now2 <= fa2._TEST_MODE_EXPIRY
