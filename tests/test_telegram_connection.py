import pytest

from services.telegram_connection import TelegramConnection
from utils.env_config import EnvConfig


def test_from_environment_reads_settings_and_builds_urls(monkeypatch):
    monkeypatch.setattr(EnvConfig, "_overrides", {"TELEGRAM_BOT_TOKEN": " tok ", "TELEGRAM_CHAT_ID": "42"})

    connection = TelegramConnection.from_environment()

    assert connection.chat_id == "42"
    assert connection.send_message_url == "https://api.telegram.org/bottok/sendMessage"
    assert connection.send_document_url.endswith("/bottok/sendDocument")


def test_missing_settings_raise(monkeypatch):
    monkeypatch.setattr(EnvConfig, "_overrides", {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""})

    with pytest.raises(RuntimeError, match="TELEGRAM_BOT_TOKEN"):
        TelegramConnection.from_environment()


def test_token_is_redacted_in_repr_and_text():
    connection = TelegramConnection(bot_token="secret", chat_id="1")

    assert "secret" not in repr(connection)
    assert connection.redact("bad secret here") == "bad [REDACTED] here"
