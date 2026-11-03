"""Upload backup ZIP files to a Telegram group via the Bot API."""

from pathlib import Path

import requests

from utils.env_config import EnvConfig


class TelegramUploadError(RuntimeError):
    """Raised when Telegram does not confirm the document upload."""


class TelegramService:
    # Conservative limit below Telegram Bot API's standard 50 MB document limit.
    MAX_DOCUMENT_BYTES = 49_000_000

    def __init__(self, bot_token: str, chat_id: str):
        if not bot_token or not chat_id:
            raise ValueError("Telegram bot token and chat ID are required")
        self._bot_token = bot_token
        self._chat_id = chat_id

    @classmethod
    def from_environment(cls) -> "TelegramService":
        token = (EnvConfig.env("TELEGRAM_BOT_TOKEN") or "").strip()
        chat_id = (EnvConfig.env("TELEGRAM_CHAT_ID") or "").strip()
        if not token or not chat_id:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID app setting")
        return cls(bot_token=token, chat_id=chat_id)

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

        url = f"https://api.telegram.org/bot{self._bot_token}/sendDocument"

        try:
            with zip_path.open("rb") as archive:
                response = requests.post(
                    url,
                    data={"chat_id": self._chat_id, "caption": caption[:1024]},
                    files={
                        "document": (
                            zip_path.name,
                            archive,
                            "application/zip",
                        )
                    },
                    timeout=(30, 600),
                )
        except requests.RequestException as exc:
            # Do not include exception URLs in logs: Telegram URLs contain bot tokens.
            raise TelegramUploadError(
                f"Telegram request failed ({type(exc).__name__}); "
                "check network connectivity and bot configuration"
            ) from None

        return self._message_id(response, "document")

    def send_message(self, text: str) -> int:
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
                data={"chat_id": self._chat_id, "text": text},
                timeout=(30, 60),
            )
        except requests.RequestException as exc:
            raise TelegramUploadError(
                f"Telegram request failed ({type(exc).__name__}); "
                "check network connectivity and bot configuration"
            ) from None

        return self._message_id(response, "message")

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
            description = description.replace(self._bot_token, "[REDACTED]")
            raise TelegramUploadError(
                f"Telegram rejected {kind} (HTTP {response.status_code}): "
                f"{description}"
            )

        return int(payload["result"]["message_id"])
