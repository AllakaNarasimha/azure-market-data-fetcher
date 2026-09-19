import os
import sys
import pytest


@pytest.fixture(autouse=True)
def ensure_repo_in_path(monkeypatch):
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
    yield
