"""Telegram digest HTML y menús con botones inline."""

from __future__ import annotations

from typing import Any

from telegram import Bot

from src.config import Settings, get_settings, repo_root


class TelegramNotifier:
    def __init__(self, settings: Settings) -> None:
        tok = settings.telegram_bot_token.strip()
        cid = settings.telegram_chat_id.strip()
        if not tok:
            raise RuntimeError(f"TELEGRAM_BOT_TOKEN vacío → {repo_root() / '.env'}")
        if not cid:
            raise RuntimeError(f"TELEGRAM_CHAT_ID vacío → {repo_root() / '.env'}")
        self._settings = settings
        self._bot = Bot(token=tok)

    def _chat_target(self, chat_id: str | None) -> str:
        return (chat_id or self._settings.telegram_chat_id).strip()

    @staticmethod
    def escape_html(text: str) -> str:
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    async def send_html(self, text: str, *, chat_id: str | None = None) -> None:
        cid = self._chat_target(chat_id)
        await self._bot.send_message(
            chat_id=cid,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    async def send_digest_hub(self, text: str, reply_markup: Any, *, chat_id: str | None = None) -> None:
        cid = self._chat_target(chat_id)
        await self._bot.send_message(
            chat_id=cid,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )

    async def send_chunked_parts(
        self,
        header: str,
        blocks: list[str],
        *,
        continuation_header: str | None = None,
        max_len: int = 3800,
        chat_id: str | None = None,
    ) -> None:
        follow = continuation_header or header
        chunk = header.rstrip() + "\n\n"
        n = len(blocks)
        for i, block in enumerate(blocks):
            suf = "\n\n" if i < n - 1 else ""
            extra = block + suf
            if len(chunk) + len(extra) > max_len and chunk.strip():
                await self.send_html(chunk.rstrip(), chat_id=chat_id)
                chunk = follow.rstrip() + "\n\n"
            chunk += extra
        if chunk.strip():
            await self.send_html(chunk.rstrip(), chat_id=chat_id)


class SilentTelegramNotifier:
    """Misma superficie asíncrona que TelegramNotifier pero sin enviar mensajes."""

    async def send_html(self, text: str, *, chat_id: str | None = None) -> None:
        _ = text, chat_id

    async def send_digest_hub(self, text: str, reply_markup: Any, *, chat_id: str | None = None) -> None:
        _ = text, reply_markup, chat_id

    async def send_chunked_parts(
        self,
        header: str,
        blocks: list[str],
        *,
        continuation_header: str | None = None,
        max_len: int = 3800,
        chat_id: str | None = None,
    ) -> None:
        _ = header, blocks, continuation_header, max_len, chat_id


def build_default_notifier() -> TelegramNotifier:
    return TelegramNotifier(settings=get_settings())
