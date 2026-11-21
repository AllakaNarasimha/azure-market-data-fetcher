from types import SimpleNamespace

import pytest

from services import market_data_backup_service as svc
from services.market_data_backup_service import MarketDataBackupService
from utils.blob_utils import BlobUtils


class FakeBlobSync:
    def __init__(self, blobs_by_container):
        self.blobs_by_container = blobs_by_container

    def get_container(self, name):
        blobs = self.blobs_by_container[name]

        class Client:
            def download_blob(self, blob_name, max_concurrency=1):
                return SimpleNamespace(chunks=lambda: iter([b"data"]))

        return Client()

    def list_blobs(self, name):
        return [SimpleNamespace(name=n) for n in self.blobs_by_container[name]]


class FakeTelegram:
    MAX_DOCUMENT_BYTES = 49_000_000

    def __init__(self, fail_document=False):
        self.fail_document = fail_document
        self.messages = []

    def send_document(self, path, caption):
        if self.fail_document:
            raise RuntimeError("telegram down")
        return 7

    def send_message(self, text):
        self.messages.append(text)
        return 8


class FakeFailedContainer:
    def __init__(self):
        self.uploaded = {}

    def get_blob_client(self, name):
        return SimpleNamespace(upload_blob=lambda data, **kw: self.uploaded.__setitem__(name, data.read()))

    def list_blobs(self):
        return []


def _service(monkeypatch, blobs, telegram, failed_container):
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: telegram))
    monkeypatch.setattr(BlobUtils, "get_container_client", staticmethod(lambda name, cs=None: failed_container))
    return MarketDataBackupService(container_name="market", blob_sync=FakeBlobSync(blobs))


class FlakyBlobSync(FakeBlobSync):
    def __init__(self, blobs_by_container, failures):
        super().__init__(blobs_by_container)
        self.failures = failures
        self.list_calls = 0

    def list_blobs(self, name):
        self.list_calls += 1
        if self.list_calls <= self.failures:
            raise ConnectionError("storage hiccup")
        return super().list_blobs(name)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(svc.time, "sleep", lambda seconds: None)


def test_zip_creation_is_retried_after_transient_error(monkeypatch):
    sync = FlakyBlobSync({"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, failures=2)
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: FakeTelegram()))
    service = MarketDataBackupService(container_name="market", blob_sync=sync)

    result = service.run()

    assert result.blob_count == 2
    assert sync.list_calls == 4


def test_zip_creation_gives_up_after_three_attempts(monkeypatch):
    sync = FlakyBlobSync({"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, failures=99)
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: FakeTelegram()))
    service = MarketDataBackupService(container_name="market", blob_sync=sync)

    with pytest.raises(ConnectionError):
        service.run()

    assert sync.list_calls == 3


def test_all_containers_empty_is_not_retried(monkeypatch):
    sync = FlakyBlobSync({"market": [], BlobUtils.BROKER_DATA_BLOB: []}, failures=0)
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: FakeTelegram()))

    with pytest.raises(RuntimeError, match="nothing to back up"):
        MarketDataBackupService(container_name="market", blob_sync=sync).run()

    assert sync.list_calls == 2


def test_failed_backup_upload_is_retried(monkeypatch):
    attempts = []

    class FlakyFailedContainer(FakeFailedContainer):
        def get_blob_client(self, name):
            def upload(data, **kw):
                attempts.append(name)
                if len(attempts) < 3:
                    raise ConnectionError("upload reset")
                self.uploaded[name] = data.read()

            return SimpleNamespace(upload_blob=upload)

    failed = FlakyFailedContainer()
    service = _service(
        monkeypatch, {"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, FakeTelegram(fail_document=True), failed
    )

    result = service.run()

    assert len(attempts) == 3
    assert result.fallback_blob in failed.uploaded


def test_empty_broker_container_does_not_block_backup(monkeypatch):
    telegram = FakeTelegram()
    service = _service(monkeypatch, {"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: []}, telegram, None)

    result = service.run()

    assert result.blob_count == 1
    assert result.telegram_message_ids == (7,)
    assert result.fallback_blob is None


def test_all_containers_empty_raises(monkeypatch):
    service = _service(monkeypatch, {"market": [], BlobUtils.BROKER_DATA_BLOB: []}, FakeTelegram(), None)

    with pytest.raises(RuntimeError, match="nothing to back up"):
        service.run()


def test_telegram_failure_saves_zip_to_failed_container(monkeypatch):
    telegram = FakeTelegram(fail_document=True)
    failed = FakeFailedContainer()
    service = _service(monkeypatch, {"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, telegram, failed)

    result = service.run()

    assert result.fallback_blob in failed.uploaded
    assert result.telegram_message_ids == ()
    assert "backup-failed" in telegram.messages[0]


def test_telegram_and_fallback_failure_raises_so_data_is_kept(monkeypatch):
    telegram = FakeTelegram(fail_document=True)
    service = _service(
        monkeypatch, {"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, telegram, None
    )

    with pytest.raises(RuntimeError, match="Cannot save failed backup"):
        service.run()

    assert "saving the full zip" in telegram.messages[0].lower()
    assert "Nothing was delivered" in telegram.messages[0]


def test_deletion_notice_says_backup_is_only_in_backup_failed(monkeypatch):
    telegram = FakeTelegram()
    service = _service(monkeypatch, {"market": ["a.csv"]}, telegram, None)
    result = svc.BackupResult("market", 1, 10, 5, (), fallback_blob="2026-11-21/market.zip")

    service.notify_container_deleted(result)

    assert "ONLY in backup-failed/2026-11-21/market.zip" in telegram.messages[0]


def test_partial_split_upload_alert_states_parts_received(monkeypatch):
    class PartialTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.sent = 0

        def send_document(self, path, caption):
            self.sent += 1
            if self.sent == 2:
                raise RuntimeError("reset")
            return self.sent

    telegram = PartialTelegram()
    telegram.MAX_DOCUMENT_BYTES = 10
    failed = FakeFailedContainer()
    service = _service(monkeypatch, {"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, telegram, failed)

    result = service.run()

    assert result.fallback_blob in failed.uploaded
    assert "1 of" in telegram.messages[0]
    assert "ONLY copy" in telegram.messages[0]
    assert "incomplete" in telegram.messages[0]


def test_zip_failure_sends_alert(monkeypatch):
    telegram = FakeTelegram()
    sync = FlakyBlobSync({"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]}, failures=99)
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: telegram))

    with pytest.raises(ConnectionError):
        MarketDataBackupService(container_name="market", blob_sync=sync).run()

    assert "ZIP creation failed" in telegram.messages[0]


def test_source_label_in_caption_and_alert(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("TEST_MODE", "true")
    monkeypatch.delenv("WEBSITE_SITE_NAME", raising=False)
    captions = []
    telegram = FakeTelegram()
    telegram.send_document = lambda path, caption: captions.append(caption) or 7
    sync = FakeBlobSync({"market": ["a.csv"], BlobUtils.BROKER_DATA_BLOB: ["b.csv"]})
    monkeypatch.setattr(svc.TelegramService, "from_environment", classmethod(lambda cls: telegram))

    MarketDataBackupService(container_name="market", blob_sync=sync).run()
    MarketDataBackupService._send_alert(telegram, "hello")

    assert "Source: dev-test" in captions[0]
    assert telegram.messages[0].startswith("Source: dev-test\n")
