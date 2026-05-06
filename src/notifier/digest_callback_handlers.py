"""Respuestas a botones inline del digest (necesita `python -m src.free_digest_app --menu-bot`)."""

from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from src.config import get_settings
from src.notifier.digest_menu_cache import load_digest_menu_payload, parse_callback
from src.notifier.digest_menu_messages import MENU_TOAST_ES, digest_menu_repeat_html
from src.notifier.digest_menu_sections import build_digest_menu_keyboard
from src.notifier.telegram_bot import TelegramNotifier

LOG = logging.getLogger(__name__)


async def on_digest_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    parsed = parse_callback(q.data)
    if not parsed:
        return
    menu_id, section = parsed
    settings = get_settings()
    expect_cid = str(settings.telegram_chat_id).strip()
    chat = update.effective_chat
    if chat is None or str(chat.id) != expect_cid:
        await q.answer("Este menú es para otro chat.", show_alert=True)
        return

    payload = load_digest_menu_payload(menu_id)
    if not payload or str(payload.chat_id) != expect_cid:
        await q.answer("Menú caducado o inválido. Espera el próximo envío.", show_alert=True)
        return

    block = payload.sections.get(section)
    if not block:
        await q.answer("Sin datos en esta sección.", show_alert=True)
        return
    blocks = block.get("blocks")
    if not isinstance(blocks, list) or not blocks:
        await q.answer("No hay contenido aquí.", show_alert=True)
        return

    toast = MENU_TOAST_ES.get(section, "Listo")
    await q.answer(toast[:200])
    header = str(block.get("header", "")).strip()
    cont = block.get("cont")
    continuation = str(cont).strip() if cont else None
    notifier = TelegramNotifier(settings)
    try:
        await notifier.send_chunked_parts(
            header,
            [str(x) for x in blocks],
            continuation_header=continuation if continuation else None,
            chat_id=str(chat.id),
        )
        keys = {k for k, v in payload.sections.items() if v.get("blocks")}
        menu_again = digest_menu_repeat_html(payload.date_iso)
        await context.bot.send_message(
            chat_id=chat.id,
            text=menu_again,
            parse_mode="HTML",
            reply_markup=build_digest_menu_keyboard(menu_id, keys),
            disable_web_page_preview=True,
        )
    except Exception as exc:
        LOG.exception("Envío Telegram menú digest: %s", exc)
        await context.bot.send_message(
            chat_id=chat.id,
            text="⚠️ Error al enviar el bloque (revisa logs).",
        )
