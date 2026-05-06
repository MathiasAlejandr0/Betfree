"""Agrupa partidos por competición para menú Telegram (botones por sección)."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.data_engine.digest_fixture import DigestFixtureRow, is_chile_primera_row
from src.notifier.digest_menu_cache import format_callback
from src.notifier.telegram_bot import TelegramNotifier

# IDs TheSportsDB → código corto (<8 chars para callback).
_TSDB_TO_SEC: dict[str, str] = {
    "4480": "ucl",
    "4481": "uel",
    "5071": "uecl",
    "4512": "usup",
    "4490": "unl",
    "4501": "lib",
    "4724": "sud",
    "5665": "rec",
    "4503": "cwc",
    "4429": "fwc",
    "4328": "eng1",
    "4335": "esp1",
    "4334": "fra1",
    "4406": "arg1",
    "4351": "bra1",
    "4627": "chi1",
}

# Ligas grandes + torneos europeos FIFA/UEFA: siempre un botón (placeholder si no hay partidos).
MENU_ALWAYS_SECTION_KEYS: frozenset[str] = frozenset(
    {
        "esp1",
        "fra1",
        "eng1",
        "chi1",
        "arg1",
        "bra1",
        "ucl",
        "uel",
        "uecl",
        "usup",
        "unl",
        "fwc",
        "cwc",
    }
)


# Orden de botones en el teclado inline.
MENU_SECTION_BUTTONS: tuple[tuple[str, str], ...] = (
    ("ag", "📅 Agenda hoy"),
    ("all", "🔮 Todas las pred."),
    ("lib", "🏆 Copa Libertadores"),
    ("sud", "🌎 Copa Sudamericana"),
    ("rec", "🔁 Recopa CONMEBOL"),
    ("ucl", "⭐ Champions League"),
    ("uel", "🅰️ Europa League"),
    ("uecl", "🅱️ Europa Conference"),
    ("usup", "🏟 Supercopa UEFA"),
    ("unl", "🌍 UEFA Nations League"),
    ("fwc", "🏆 Mundial FIFA · mayores"),
    ("cwc", "🏅 Mundial de clubes"),
    ("eng1", "🏴 Inglaterra · Premier"),
    ("esp1", "🇪🇸 España · La Liga"),
    ("fra1", "🇫🇷 Francia · Ligue 1"),
    ("chi1", "🇨🇱 Chile · Primera"),
    ("arg1", "🇦🇷 Argentina · Liga"),
    ("bra1", "🇧🇷 Brasil · Serie A"),
    ("oth", "📎 Otras ligas"),
)

SECTION_LABEL_ES: dict[str, str] = {k: v for k, v in MENU_SECTION_BUTTONS}


def ensure_always_menu_sections(sections: dict[str, dict[str, Any]], *, date_iso: str) -> None:
    """Inserta mensaje útil donde no hubo datos (botón visible igual)."""
    esc = TelegramNotifier.escape_html(date_iso)
    for key in MENU_ALWAYS_SECTION_KEYS:
        label = SECTION_LABEL_ES.get(key, key)
        body = (
            f"ℹ️ <b>Sin partidos con hora hoy</b>\n"
            f"<b>{TelegramNotifier.escape_html(label)}</b> · <code>{esc}</code>\n\n"
            "En cuanto ESPN o TheSportsDB tengan fechas locales para esta competición, "
            "verás partidos aquí.\n\n"
            "El Mundial mayores aparece en <code>fifa.world</code> cuando esté más cerca;"
            " la clasificación <code>fifa.worldq.*</code> se agrupa con el mismo botón."
        )
        if key not in sections:
            sections[key] = {
                "header": f"<b>{label}</b> <code>{esc}</code>\n",
                "cont": None,
                "blocks": [body],
            }
        else:
            bl = sections[key].get("blocks")
            if not bl:
                sections[key]["blocks"] = [body]


def digest_menu_section(row: DigestFixtureRow) -> str:
    """Código de sección para callback (2–7 chars)."""
    s = (row.slug or "").strip().lower()
    if s == "uefa.champions":
        return "ucl"
    if s == "uefa.europa":
        return "uel"
    if s == "uefa.europa.conf":
        return "uecl"
    if s == "uefa.super_cup":
        return "usup"
    if s == "uefa.nations":
        return "unl"
    if s == "conmebol.libertadores":
        return "lib"
    if s == "conmebol.sudamericana":
        return "sud"
    if s == "conmebol.recopa":
        return "rec"
    if s == "fifa.cwc":
        return "cwc"
    # Mundial FIFA (final u otro formato); clasificaciones bajo mismo botón para el usuario.
    if s == "fifa.world" or s.startswith("fifa.worldq"):
        return "fwc"

    if s == "eng.1":
        return "eng1"
    if s == "esp.1":
        return "esp1"
    if s == "fra.1":
        return "fra1"
    if s == "chi.1":
        return "chi1"
    if s == "arg.1":
        return "arg1"
    if s == "bra.1":
        return "bra1"
    if s.startswith("tsdb."):
        lid = s.removeprefix("tsdb.").strip()
        hit = _TSDB_TO_SEC.get(lid)
        if hit is not None:
            return hit

    ln = (row.league_name or "").casefold()
    if any(k in ln for k in ("libertadores", "copa libertadores")):
        return "lib"
    if any(k in ln for k in ("sudamericana", "copa sudamericana")) and "recopa" not in ln:
        return "sud"
    if "recopa conmebol" in ln or (ln.endswith("recopa") and ("sudamer" in ln or "conmebol" in ln)):
        return "rec"
    if "recopa" in ln:
        return "rec"
    if "champions league" in ln or "uefa champions" in ln:
        return "ucl"
    if ("europa league" in ln or "ligue europa" in ln) and "conference" not in ln:
        return "uel"
    if "conference league" in ln or ("uefa europa conference" in ln):
        return "uecl"
    if "super cup" in ln and "uefa" in ln:
        return "usup"
    if "nations league" in ln:
        return "unl"
    if ("world cup" in ln or "copa mundial" in ln) and "club" not in ln:
        if any(w in ln for w in ("women", "womens", "femenino", "femenina")):
            return "oth"
        return "fwc"
    if "club world cup" in ln or "mundial de clubes" in ln:
        return "cwc"

    if "premier league" in ln and "rugby" not in ln:
        return "eng1"
    if "la liga" in ln or "laliga" in ln or "spanish la liga" in ln:
        return "esp1"
    if "ligue 1" in ln or "ligue un" in ln:
        return "fra1"

    if is_chile_primera_row(row):
        return "chi1"
    if "argentina" in ln and (
        "primera" in ln or "profesional" in ln or "liga profesional" in ln or "liga profe" in ln
    ):
        return "arg1"
    if "brasileir" in ln or ("brazil" in ln and "serie a" in ln):
        return "bra1"

    return "oth"


def section_sort_key(sec: str) -> int:
    order = [k for k, _ in MENU_SECTION_BUTTONS]
    try:
        return order.index(sec)
    except ValueError:
        return 999


def build_digest_menu_keyboard(menu_id: str, section_keys_with_data: set[str]) -> InlineKeyboardMarkup:
    """Un botón por fila: más fácil de leer y de pulsar en el teléfono."""
    rows: list[list[InlineKeyboardButton]] = []
    for sec, label in MENU_SECTION_BUTTONS:
        if sec not in section_keys_with_data:
            continue
        cb = format_callback(menu_id, sec)
        if len(cb.encode("utf-8")) > 64:
            raise ValueError("callback_data supera el límite de Telegram (64 bytes)")
        rows.append([InlineKeyboardButton(label, callback_data=cb)])
    return InlineKeyboardMarkup(rows)
