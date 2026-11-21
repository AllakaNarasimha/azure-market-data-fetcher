import pytest
import requests

from services import telegram_service
from services.telegram_service import TelegramService, TelegramUploadError


def test_failed_upload_reports_reason_without_token(monkeypatch, tmp_path):
    zip_path = tmp_path / "a.zip"
    zip_path.write_bytes(b"data")

    def reset(url, **kwargs):
        raise requests.ConnectionError(f"Connection reset by peer for {url}")

    monkeypatch.setattr(telegram_service.requests, "post", reset)
    monkeypatch.setattr(telegram_service.time, "sleep", lambda seconds: None)

    with pytest.raises(TelegramUploadError) as caught:
        TelegramService("secret-token", "1").send_document(zip_path, "caption")

    assert "Connection reset by peer" in str(caught.value)
    assert "secret-token" not in str(caught.value)
