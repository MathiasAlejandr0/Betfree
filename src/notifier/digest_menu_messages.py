"""Textos HTML del menú Telegram (legibles y coherentes en móvil)."""

from __future__ import annotations

from src.notifier.telegram_bot import TelegramNotifier

# Retroalimentación rápida al pulsar (Telegram limita ~200 caracteres).
MENU_TOAST_ES: dict[str, str] = {
    "ag": "📅 Agenda enviada",
    "all": "🔮 Pronósticos (todo)",
    "lib": "🏆 Libertadores",
    "sud": "🌎 Sudamericana",
    "rec": "🔁 Recopa CONMEBOL",
    "ucl": "⭐ Champions",
    "uel": "🅰️ Europa League",
    "uecl": "🅱️ Conference League",
    "usup": "🏟 Supercopa UEFA",
    "unl": "🌍 Nations League",
    "fwc": "🏆 Mundial FIFA",
    "cwc": "🏅 Mundial de clubes",
    "eng1": "🏴 Inglaterra",
    "esp1": "🇪🇸 España",
    "fra1": "🇫🇷 Francia",
    "chi1": "🇨🇱 Chile",
    "arg1": "🇦🇷 Argentina",
    "bra1": "🇧🇷 Brasil",
    "oth": "📎 Otras ligas",
}


def digest_menu_intro_html(date_iso: str, *, show_listener_note: bool = True) -> str:
    """Primer mensaje del día con teclado inline."""
    esc = TelegramNotifier.escape_html(date_iso)
    note = ""
    if show_listener_note:
        note = (
            "\n━━━━━━━━━━━━━━\n"
            "<b>Aviso técnico</b>\n"
            "Los botones solo funcionan si en el PC/servidor corre:\n"
            "<code>python -m src.free_digest_app --menu-bot</code>\n"
        )
    return (
        f"🤖 <b>Betfree</b> · menú del día\n\n"
        f"📆 <code>{esc}</code>\n\n"
        "<b>Elegí qué querés ver</b>\n"
        "Te lo mando en mensajes abajo.\n\n"
        "Siempre aparecen botones para <b>ES / FR / ENG / CHI / ARG / BRA</b>, "
        "<b>UCL · UEL · Conference · Supercopa UEFA · Nations League</b>, "
        "<b>Mundial FIFA (mayores)</b> y <b>Mundial de clubes</b>; "
        "si hoy no hay partidos ahí verás un aviso breve.\n\n"
        "Cuando termines de leer un bloque, te <b>vuelvo a dejar este menú</b> para otra opción.\n"
        f"{note}"
        "\n👇 <b>Tocá un botón</b>"
    )


def digest_menu_repeat_html(date_iso: str) -> str:
    """Menú que reaparece tras cada pulsación (misma caché / mismo día)."""
    esc = TelegramNotifier.escape_html(date_iso)
    return (
        f"──────────────\n"
        f"🤖 <b>Otra vez el menú</b>\n\n"
        f"Mismo día: <code>{esc}</code>\n\n"
        "Los datos no cambian hasta el próximo digest.\n"
        "<b>Elegí otra sección</b> cuando quieras 👇"
    )
