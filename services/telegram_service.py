"""Upload backup ZIP files to a Telegram group via the Bot API."""

import logging
import time
from pathlib import Path

import requests

from services.telegram_connection import TelegramConnection

logger = logging.getLogger(__name__)


class TelegramUploadError(RuntimeError):
    """Raised when Telegram does not confirm the document upload."""



class TelegramService:
    # Conservative limit below Telegram Bot API's standard 50 MB document limit.
    MAX_DOCUMENT_BYTES = 49_000_000
    # Flaky connectivity (e.g. mid-upload resets) is common for large uploads; retry before giving up.
    MAX_SEND_ATTEMPTS = 3
    RETRY_BACKOFF_SECONDS = 5

    def __init__(self, bot_token: str, chat_id: str):
        self._connection = TelegramConnection(bot_token=bot_token, chat_id=chat_id)

    @classmethod
    def from_environment(cls) -> "TelegramService":
        connection = TelegramConnection.from_environment()
        return cls(bot_token=connection.bot_token, chat_id=connection.chat_id)

    def send_document(self, zip_path: Path, caption: str) -> int:
        """Send a ZIP; return the Telegram message ID on success."""
        if not zip_path.is_file():
            raise FileNotFoundError(f"Backup ZIP not found: {zip_path}")

        size = zip_path.stat().st_size
        if size > self.MAX_DOCUMENT_BYTES:
            raise TelegramUploadError(
                f"ZIP is {size:,} bytes; exceeds the conservative "
                f"{self.MAX_DOCUMENT_BYTES:,}-byte Telegram limit"
            )

        url = self._connection.send_document_url

        for attempt in range(1, self.MAX_SEND_ATTEMPTS + 1):
            try:
                with zip_path.open("rb") as archive:
                    response = requests.post(
                        url,
                        data={"chat_id": self._connection.chat_id, "caption": caption[:1024]},
                        files={
                            "document": (
                                zip_path.name,
                                archive,
                                "application/zip",
                            )
                        },
                        timeout=(30, 600),
                    )
                return self._message_id(response, "document")
            except requests.RequestException as exc:
                # The reason shows why the upload failed (reset, broken pipe); the token is redacted.
                reason = self._connection.redact(str(exc))[:300]
                if attempt == self.MAX_SEND_ATTEMPTS:
                    # Do not include exception URLs in logs: Telegram URLs contain bot tokens.
                    raise TelegramUploadError(
                        f"Telegram request failed ({type(exc).__name__}: {reason}) after {attempt} attempts; "
                        "check network connectivity and bot configuration"
                    ) from None
                logger.warning(
                    "[Telegram] Document upload attempt %d/%d failed (%s: %s); retrying in %ds",
                    attempt, self.MAX_SEND_ATTEMPTS, type(exc).__name__, reason, self.RETRY_BACKOFF_SECONDS * attempt,
                )
                time.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

    def send_message(self, text: str) -> int:
        url = self._connection.send_message_url

        for attempt in range(1, self.MAX_SEND_ATTEMPTS + 1):
            try:
                response = requests.post(
                    url,
                    data={"chat_id": self._connection.chat_id, "text": text},
                    timeout=(30, 60),
                )
                return self._message_id(response, "message")
            except requests.RequestException as exc:
                if attempt == self.MAX_SEND_ATTEMPTS:
                    raise TelegramUploadError(
                        f"Telegram request failed ({type(exc).__name__}) after {attempt} attempts; "
                        "check network connectivity and bot configuration"
                    ) from None
                logger.warning(
                    "[Telegram] Message send attempt %d/%d failed (%s); retrying in %ds",
                    attempt, self.MAX_SEND_ATTEMPTS, type(exc).__name__, self.RETRY_BACKOFF_SECONDS * attempt,
                )
                time.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

    def _message_id(self, response: requests.Response, kind: str) -> int:
        try:
            payload = response.json()
        except ValueError:
            raise TelegramUploadError(
                f"Telegram returned non-JSON response (HTTP {response.status_code})"
            ) from None

        if response.status_code != 200 or payload.get("ok") is not True:
            description = str(payload.get("description", "Unknown Telegram error"))
            # Redact token defensively even if a remote error mentions it.
            description = self._connection.redact(description)
            raise TelegramUploadError(
                f"Telegram rejected {kind} (HTTP {response.status_code}): "
                f"{description}"
            )

        return int(payload["result"]["message_id"])
