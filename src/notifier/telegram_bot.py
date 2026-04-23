"""Telegram alert sender for betting opportunities."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from telegram import Bot
from telegram.error import TelegramError

from src.config import Settings, get_settings


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BetAlert:
    """Serializable alert object."""

    match_name: str
    suggested_market: str
    probability: float
    odds: float
    stake: float


class TelegramNotifier:
    """Telegram bot notifier."""

    def __init__(self, settings: Settings) -> None:
        if not settings.telegram_bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured.")
        if not settings.telegram_chat_id:
            raise RuntimeError("TELEGRAM_CHAT_ID is not configured.")
        self._settings = settings
        self._bot = Bot(token=settings.telegram_bot_token)

    def format_alert_message(self, alert: BetAlert) -> str:
        """Build markdown message for Telegram."""
        return (
            "🚨 *Alerta de Value Bet*\n\n"
            f"⚽ *Partido:* {alert.match_name}\n"
            f"🎯 *Mercado sugerido:* {alert.suggested_market}\n"
            f"📊 *Probabilidad modelo:* {alert.probability * 100:.2f}%\n"
            f"💸 *Cuota:* {alert.odds:.2f}\n"
            f"🏦 *Stake recomendado:* {alert.stake:.2f} u"
        )

    async def send_alert(self, alert: BetAlert) -> str:
        """Send alert and return message body."""
        message = self.format_alert_message(alert)
        try:
            await self._bot.send_message(
                chat_id=self._settings.telegram_chat_id,
                text=message,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except TelegramError as exc:
            LOGGER.error("Telegram send failure: %s", exc)
            raise
        return message


def build_default_notifier() -> TelegramNotifier:
    """Build notifier from environment settings."""
    return TelegramNotifier(settings=get_settings())

