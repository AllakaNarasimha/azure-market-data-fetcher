"""Telegram Bot API connection details: credentials, endpoints and where they are read from."""

from dataclasses import dataclass

from utils.env_config import EnvConfig

API_BASE_URL = "https://api.telegram.org"


@dataclass(frozen=True)
class TelegramConnection:
    bot_token: str = ""
    chat_id: str = ""

    def __post_init__(self) -> None:
        if not self.bot_token or not self.chat_id:
            raise ValueError("Telegram bot token and chat ID are required")

    @classmethod
    def from_environment(cls) -> "TelegramConnection":
        token = (EnvConfig.env("TELEGRAM_BOT_TOKEN") or "").strip()
        chat_id = (EnvConfig.env("TELEGRAM_CHAT_ID") or "").strip()
        if not token or not chat_id:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID app setting")
        return cls(bot_token=token, chat_id=chat_id)

    @property
    def send_document_url(self) -> str:
        return f"{API_BASE_URL}/bot{self.bot_token}/sendDocument"

    @property
    def send_message_url(self) -> str:
        return f"{API_BASE_URL}/bot{self.bot_token}/sendMessage"

    def redact(self, text: str) -> str:
        """Remove the bot token from text that may be logged or shown."""
        return text.replace(self.bot_token, "[REDACTED]")

    def __repr__(self) -> str:
        return f"TelegramConnection(chat_id={self.chat_id!r}, bot_token='[REDACTED]')"
