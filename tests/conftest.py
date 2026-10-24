import json
import os
import sys
import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _load_local_settings_into_env():
    if os.environ.get("MARKET_STORAGE_CONNECTION"):
        return
    settings_path = os.path.join(REPO_ROOT, "local.settings.json")
    if not os.path.exists(settings_path):
        return
    with open(settings_path, encoding="utf-8") as fh:
        values = json.load(fh).get("Values", {})
    conn = values.get("MARKET_STORAGE_CONNECTION")
    if conn:
        os.environ["MARKET_STORAGE_CONNECTION"] = conn


def pytest_configure(config):
    config.addinivalue_line("markers", "live_blob: calls real Azure Blob storage (MARKET_STORAGE_CONNECTION)")
    _load_local_settings_into_env()


@pytest.fixture(autouse=True)
def ensure_repo_in_path(monkeypatch):
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
    yield
