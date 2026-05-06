"""Digest ESPN + CSV + Telegram."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import DEFAULT_FREE_ESPN_SLUGS, Settings, get_settings
from src.data_engine.digest_fixture import DigestFixtureRow, is_chile_primera_row, merge_digest_fixtures
from src.data_engine.espn_free_fixtures import EspnFixtureRow, fetch_fixtures_day
from src.data_engine.thesportsdb_fixtures import fetch_thesportsdb_first_tier_day, load_first_tier_league_ids_for_day
from src.notifier.digest_menu_cache import DigestMenuPayload, save_digest_menu_payload
from src.notifier.digest_menu_messages import digest_menu_intro_html
from src.notifier.digest_menu_sections import (
    SECTION_LABEL_ES,
    build_digest_menu_keyboard,
    digest_menu_section,
    ensure_always_menu_sections,
)
from src.notifier.telegram_bot import TelegramNotifier
from src.predictor.csv_roll_state import estimated_match_cards_split, estimated_match_corners_split
from src.predictor.digest_hist_league_map import hist_competition_for_digest_slug
from src.predictor.digest_roll_context import DigestRollContext
from src.predictor.e0_digest_predictor import E0DigestPredictor
from src.predictor.probability_blend import convex_blend_1x2
from src.predictor.reliability_signals import digest_reliability_html_line
from src.storage.repository import TimeSeriesRepository

LOG = logging.getLogger(__name__)

_SLUG_DIGEST_RANK: dict[str, int] = {s: i for i, s in enumerate(DEFAULT_FREE_ESPN_SLUGS)}


def _slug_digest_rank(slug: str) -> int:
    s = (slug or "").strip()
    if s.startswith("tsdb."):
        return 400
    return _SLUG_DIGEST_RANK.get(s, 900)


# Solo competiciones internacionales de clubes relevantes Europa / Sudamérica (ESPN).
_INTERNATIONAL_ESPN_SLUG_PREFIXES: tuple[str, ...] = (
    "uefa.",
    "conmebol.",
    "fifa.",
)

_INTERNATIONAL_ESPN_SLUG_EXACT: frozenset[str] = frozenset()

# Prefijos ESPN fuera de Europa + Sudamérica (si DIGEST_ESPN_EU_SA_ONLY=true).
# TheSportsDB idLeague: mismas categorías prioritarias que en agenda (EU + CONMEBOL copas + grandes ligas).
_TSDB_FORCE_FIRST_LEAGUE_IDS: frozenset[str] = frozenset(
    {
        "4328",  # Premier League
        "4335",  # La Liga
        "4334",  # Ligue 1
        "4480",  # UCL
        "4481",  # UEL
        "5071",  # UECL
        "4512",  # Super Cup
        "4490",  # Nations League
        "4429",  # Mundial FIFA
        "4406",  # Argentina Primera
        "4351",  # Brasil Serie A
        "4627",  # Chile Primera
        "4501",  # Copa Libertadores
        "4724",  # Copa Sudamericana
        "5665",  # Recopa Sudamericana
        "4503",  # FIFA Club World Cup
    }
)

_ESPN_SLUG_PREFIX_BLOCK_NON_EU_SA: tuple[str, ...] = (
    "usa.",
    "can.",
    "mex.",
    "jpn.",
    "kor.",
    "chn.",
    "aus.",
    "nzl.",
    "ind.",
    "tha.",
    "mys.",
    "idn.",
    "sgp.",
    "ksa.",
    "irn.",
    "qat.",
    "are.",
    "afc.",
    "caf.",
    "concacaf.",
    "global.",
    "campeones.",
)


def _slug_allowed_for_digest(slug: str, settings: Settings) -> bool:
    if not settings.free_espn_first_division_only:
        return True
    s = (slug or "").strip().lower()
    if s.startswith(_INTERNATIONAL_ESPN_SLUG_PREFIXES) or s in _INTERNATIONAL_ESPN_SLUG_EXACT:
        return True
    parts = s.split(".")
    if len(parts) == 2 and parts[1] == "1":
        return True
    return s in settings.free_espn_extra_first_tier_cup_slugs


def _slug_allowed_espn_eu_sa(slug: str, settings: Settings) -> bool:
    if not settings.digest_espn_eu_sa_only:
        return True
    s = (slug or "").strip().lower()
    if s.startswith("tsdb."):
        return True
    return not any(s.startswith(p) for p in _ESPN_SLUG_PREFIX_BLOCK_NON_EU_SA)


def _needles_cf(settings: Settings) -> tuple[str, ...]:
    return tuple(n.casefold() for n in settings.fixture_digest_team_priority if str(n).strip())


def _row_matches_tracked_team(row: DigestFixtureRow, needles_cf: tuple[str, ...]) -> bool:
    if not needles_cf:
        return False
    h = row.home_name.casefold()
    a = row.away_name.casefold()
    return any(n in h or n in a for n in needles_cf)


def _row_is_digest_force_first(row: DigestFixtureRow, settings: Settings) -> bool:
    """Copas internacionales de clubes + ligas objetivo (ESPN/TSDB/heurística nombre)."""
    s = (row.slug or "").strip().lower()
    if s.startswith(("conmebol.", "uefa.", "fifa.")):
        return True
    if s in settings.digest_force_first_es_slugs:
        return True
    if s.startswith("tsdb."):
        lid = s.removeprefix("tsdb.").strip()
        if lid in _TSDB_FORCE_FIRST_LEAGUE_IDS:
            return True
    ln = (row.league_name or "").casefold()
    if is_chile_primera_row(row):
        return True
    if any(
        k in ln
        for k in (
            "libertadores",
            "sudamericana",
            "recopa sudamericana",
            "champions league",
            "europa league",
            "conference league",
            "club world cup",
        )
    ):
        return True
    if "argentina" in ln and ("primera" in ln or "profesional" in ln or "liga profesional" in ln):
        return True
    if "brasileir" in ln or ("brazil" in ln and "serie a" in ln):
        return True
    return False


def _prioritized_fixture_list(fx_candidates: list[DigestFixtureRow], cap: int, settings: Settings) -> list[DigestFixtureRow]:
    needles = _needles_cf(settings)

    def kick(r: DigestFixtureRow) -> datetime:
        return r.kickoff_utc or datetime.min.replace(tzinfo=timezone.utc)

    if not settings.digest_force_first_priority:
        chi = [r for r in fx_candidates if is_chile_primera_row(r)]
        tracked = [r for r in fx_candidates if not is_chile_primera_row(r) and _row_matches_tracked_team(r, needles)]
        rest = [
            r
            for r in fx_candidates
            if not is_chile_primera_row(r) and not _row_matches_tracked_team(r, needles)
        ]
        chi.sort(key=kick)
        tracked.sort(key=lambda r: (_slug_digest_rank(r.slug), kick(r)))
        rest.sort(key=lambda r: (_slug_digest_rank(r.slug), kick(r)))
        merged_order = chi + tracked + rest
        if len(merged_order) > cap:
            LOG.info(
                "Agenda acotada a %s partidos (%s fuera por DAILY_DIGEST_MAX_FIXTURES).",
                cap,
                len(merged_order) - cap,
            )
        return merged_order[:cap]

    force = [r for r in fx_candidates if _row_is_digest_force_first(r, settings)]
    force.sort(key=kick)
    seen: set[int] = {r.event_id for r in force}
    rest_pool = [r for r in fx_candidates if r.event_id not in seen]

    chi = [r for r in rest_pool if is_chile_primera_row(r)]
    tracked = [r for r in rest_pool if not is_chile_primera_row(r) and _row_matches_tracked_team(r, needles)]
    rest = [
        r
        for r in rest_pool
        if not is_chile_primera_row(r) and not _row_matches_tracked_team(r, needles)
    ]
    chi.sort(key=kick)
    tracked.sort(key=lambda r: (_slug_digest_rank(r.slug), kick(r)))
    rest.sort(key=lambda r: (_slug_digest_rank(r.slug), kick(r)))
    merged_order = force + chi + tracked + rest
    if len(merged_order) > cap:
        LOG.info(
            "Agenda acotada a %s partidos (%s fuera); primero Libertadores/Sudamericana/UEFA/FIFA + ligas CHI–ARG–BRA–ESP–ENG–FRA.",
            cap,
            len(merged_order) - cap,
        )
    return merged_order[:cap]


def _prediction_slice(fx: list[DigestFixtureRow], pred_cap: int, settings: Settings) -> list[DigestFixtureRow]:
    """Misma lista que la agenda, pero si hay que recortar pronósticos, cubrir antes copas intl + ligas prioridad."""

    def kick(r: DigestFixtureRow) -> datetime:
        return r.kickoff_utc or datetime.min.replace(tzinfo=timezone.utc)

    if pred_cap <= 0:
        return []
    if not settings.digest_force_first_priority:
        return fx[:pred_cap]
    hi = [r for r in fx if _row_is_digest_force_first(r, settings)]
    hi.sort(key=kick)
    lo = [r for r in fx if not _row_is_digest_force_first(r, settings)]
    return (hi + lo)[:pred_cap]


@dataclass(frozen=True)
class PipelineConfig:
    bankroll: float = 1000.0


def _cron(expr: str) -> dict[str, Any]:
    p = expr.strip().split()
    if len(p) != 5:
        raise ValueError("Cron requiere 5 campos.")
    return dict(zip(("minute", "hour", "day", "month", "day_of_week"), p))


def build_scheduler(*, async_job: Callable[[], None], cron_expression: str) -> BlockingScheduler:
    sch = BlockingScheduler()
    sch.add_job(async_job, "cron", **_cron(cron_expression), max_instances=1, coalesce=True)
    return sch


def _kickoff_local_display(row: DigestFixtureRow) -> str:
    if row.kickoff_utc is None:
        return "—"
    loc = row.kickoff_utc.astimezone()
    return loc.strftime("%H:%M")


def _digest_matches_local_day(row: EspnFixtureRow | DigestFixtureRow, local_day: date) -> bool:
    # Sin UTC fiable no podemos asignar el partido a "hoy"; antes se incluían todos los TBD y contaminaban el digest.
    if row.kickoff_utc is None:
        return False
    return row.kickoff_utc.astimezone().date() == local_day


def _pred_with_optional_e0_ml(
    row: DigestFixtureRow,
    roll_ctx: DigestRollContext,
    e0p: E0DigestPredictor | None,
    settings: Settings,
) -> tuple[float, float, float, float, float, bool, float]:
    """1X2, goles esperados Poisson+Elo y metadatos: (used_ml, ml_weight aplicado si hubo ML, 0 si no)."""
    hist = hist_competition_for_digest_slug(row.slug)
    st0 = roll_ctx.pick_state(hist)
    ph0, pd0, pa0, xh, xa = st0.poisson_probs(row.home_name, row.away_name)
    w_ml = float(settings.digest_e0_ml_blend_weight)
    if e0p is not None and e0p.active:
        pm = e0p.probs_1x2_for_row(row)
        if pm is not None:
            if w_ml >= 0.999:
                return pm[0], pm[1], pm[2], xh, xa, True, 1.0
            bh, bd, ba = convex_blend_1x2(pm, (ph0, pd0, pa0), w_ml)
            return bh, bd, ba, xh, xa, True, w_ml
    return ph0, pd0, pa0, xh, xa, False, 0.0


def _winner_label(home: str, away: str, ph: float, pd_: float, pa: float) -> str:
    if max(ph, pd_, pa) == ph:
        return home
    if max(ph, pd_, pa) == pa:
        return away
    return "Empate"


def _pct(p: float) -> str:
    return f"{100.0 * p:.1f}%"


def _digest_block_sep() -> str:
    return "<code>────────────</code>\n\n"


def _prediction_block_compact(
    match_idx: int,
    row: DigestFixtureRow,
    roll_ctx: DigestRollContext,
    e0p: E0DigestPredictor | None,
    settings: Settings,
) -> str:
    e = TelegramNotifier.escape_html
    hist = hist_competition_for_digest_slug(row.slug)
    st_aux = roll_ctx.pick_state(hist)
    ph, pd_, pa, xh, xa, used_ml, blend_ml_w = _pred_with_optional_e0_ml(row, roll_ctx, e0p, settings)
    w = _winner_label(row.home_name, row.away_name, ph, pd_, pa)
    goals_match = xh + xa
    yh, ya, yt = estimated_match_cards_split(st_aux, row.home_name, row.away_name, goals_match, xh, xa)
    kh, ka, kt = estimated_match_corners_split(st_aux, row.home_name, row.away_name, goals_match, xh, xa)
    hn, an = e(row.home_name), e(row.away_name)
    wg = e(w)
    lg = e((row.league_name or "").strip() or "—")
    if used_ml:
        if blend_ml_w >= 0.995:
            ml_note = " ·<code>1X2</code> modelo tabular E0"
        else:
            ml_note = f" ·<code>1X2</code> E0+Poisson {int(round(100 * blend_ml_w))}% ML"
    else:
        ml_note = ""
    rel = ""
    if settings.digest_show_reliability_hint:
        rel = "\n" + digest_reliability_html_line(
            ph,
            pd_,
            pa,
            used_ml=used_ml,
            blend_ml_w=blend_ml_w,
            pct_fmt=_pct(max(ph, pd_, pa)),
            poisson_basis="liga" if roll_ctx.is_isolated_league(hist) else "global",
        )
    lead = _digest_block_sep() if match_idx > 1 else ""
    return (
        lead
        + f"<b>#{match_idx}</b> ⚽ <b>{hn}</b> · <b>{an}</b>\n"
        + f"🏟 {lg}\n"
        + f"🏆 <b>Favorito</b>: {wg}{ml_note}\n"
        + f"📊 <b>1X2</b>: Local {_pct(ph)} · Empate {_pct(pd_)} · Visita {_pct(pa)}\n"
        + f"🥅 <b>Goles</b> (esperanza): {xh:.2f} – {xa:.2f} · Total {goals_match:.2f}\n"
        + f"🟨 <b>Tarjetas</b> (amarillas aprox.): {yh:.1f} – {ya:.1f} · Partido {yt:.1f}\n"
        + f"📐 <b>Corners</b> (esperanza): {kh:.1f} – {ka:.1f} · Total {kt:.1f}"
        + rel
    )


def _agenda_line_blocks(fx: list[DigestFixtureRow]) -> list[str]:
    blocks: list[str] = []
    for i, r in enumerate(fx, 1):
        h, a = TelegramNotifier.escape_html(r.home_name), TelegramNotifier.escape_html(r.away_name)
        lg = TelegramNotifier.escape_html(r.league_name)
        tt = TelegramNotifier.escape_html(_kickoff_local_display(r))
        lead = _digest_block_sep() if i > 1 else ""
        blocks.append(
            lead
            + f"<b>{i}.</b> 🕐 <code>{tt}</code>\n"
            + f"<b>{h}</b> – <b>{a}</b>\n"
            + f"🏆 {lg}"
        )
    return blocks


async def run_free_daily_digest(
    notifier: TelegramNotifier,
    repo: TimeSeriesRepository,
    settings: Settings,
) -> None:
    today_local = date.today()
    roll_ctx = DigestRollContext.from_csv(
        settings.historical_csv_path,
        before_day=today_local,
        draw_calibration_factor=settings.digest_poisson_draw_factor,
        per_league_enabled=settings.digest_per_league_poisson,
        min_rows_per_league=settings.digest_per_league_min_rows,
    )
    e0p = E0DigestPredictor(settings)
    e0p.prepare(today_local)
    merged: dict[int, EspnFixtureRow] = {}
    radius = max(1, settings.fixture_digest_fetch_day_radius)
    for slug in settings.free_espn_soccer_slugs:
        for delta in range(-radius, radius + 1):
            day = today_local + timedelta(days=delta)
            try:
                rows, _ = await asyncio.to_thread(fetch_fixtures_day, settings, slug, day)
            except Exception as exc:
                LOG.warning("ESPN %s %s: %s", slug, day, exc)
                continue
            for r in rows:
                merged[r.event_id] = r
            await asyncio.sleep(settings.min_request_interval_seconds)

    primary_slugs = set(settings.free_espn_soccer_slugs)
    if settings.digest_fetch_global_espn_today:
        for slug in settings.free_espn_global_tier1_slugs:
            if slug in primary_slugs:
                continue
            if not _slug_allowed_for_digest(slug, settings):
                continue
            try:
                rows, _ = await asyncio.to_thread(fetch_fixtures_day, settings, slug, today_local)
            except Exception as exc:
                LOG.debug("ESPN global %s: %s", slug, exc)
                continue
            for r in rows:
                merged[r.event_id] = r
            await asyncio.sleep(settings.min_request_interval_seconds)

    espn_digest = [
        DigestFixtureRow.from_espn(r)
        for r in merged.values()
        if _digest_matches_local_day(r, today_local)
        and _slug_allowed_for_digest(r.slug, settings)
        and _slug_allowed_espn_eu_sa(r.slug, settings)
    ]
    ts_rows: list[DigestFixtureRow] = []
    if settings.digest_use_thesportsdb:
        try:
            league_ids = await asyncio.to_thread(load_first_tier_league_ids_for_day, settings, today_local)
            await asyncio.sleep(settings.min_request_interval_seconds)
            if league_ids:
                ts_rows = await asyncio.to_thread(fetch_thesportsdb_first_tier_day, settings, today_local, league_ids)
        except Exception as exc:
            LOG.warning("TheSportsDB digest: %s", exc)
    fx_candidates = merge_digest_fixtures(espn_digest, ts_rows)
    cap = max(1, settings.daily_digest_max_fixtures)
    fx = _prioritized_fixture_list(fx_candidates, cap, settings)

    esc = TelegramNotifier.escape_html(today_local.isoformat())
    head = (
        f"📅 <b>Agenda</b> <code>{esc}</code>\n"
        f"ESPN + TheSportsDB · 1ª EU/SA · UEFA / CONMEBOL / FIFA CWC\n"
    )
    cont = f"📅 <b>Agenda (cont.)</b> <code>{esc}</code>\n"
    if not fx:
        await notifier.send_html(
            head + "⚠️ Sin partidos con hora para hoy (revisa <code>DIGEST_USE_THESPORTSDB</code> y slugs ESPN en <code>.env</code>)."
        )
        return
    agenda_blocks = _agenda_line_blocks(fx)
    agenda_idx = {r.event_id: i for i, r in enumerate(fx, 1)}

    if settings.telegram_digest_menu:

        def _kick_ord(r: DigestFixtureRow) -> datetime:
            return r.kickoff_utc or datetime.min.replace(tzinfo=timezone.utc)

        by_sec: dict[str, list[DigestFixtureRow]] = defaultdict(list)
        for r in fx:
            by_sec[digest_menu_section(r)].append(r)
        for lst in by_sec.values():
            lst.sort(key=_kick_ord)

        sections_payload: dict[str, dict[str, Any]] = {
            "ag": {"header": head, "cont": cont, "blocks": agenda_blocks},
            "all": {
                "header": (
                    f"🔮 <b>Todas las predicciones</b> <code>{esc}</code>\n"
                    "1X2 · goles · tarjetas · corners · orden de agenda\n"
                ),
                "cont": f"🔮 <b>Todas (cont.)</b> <code>{esc}</code>\n",
                "blocks": [_prediction_block_compact(agenda_idx[r.event_id], r, roll_ctx, e0p, settings) for r in fx],
            },
        }
        for sec, rows in by_sec.items():
            if not rows:
                continue
            label = SECTION_LABEL_ES.get(sec, sec)
            sections_payload[sec] = {
                "header": f"🔮 <b>{label}</b> <code>{esc}</code>\n",
                "cont": f"🔮 <b>{label}</b> (cont.) <code>{esc}</code>\n",
                "blocks": [_prediction_block_compact(agenda_idx[r.event_id], r, roll_ctx, e0p, settings) for r in rows],
            }

        ensure_always_menu_sections(sections_payload, date_iso=today_local.isoformat())
        keys = {k for k, v in sections_payload.items() if v.get("blocks")}
        payload = DigestMenuPayload(
            chat_id=str(settings.telegram_chat_id).strip(),
            date_iso=today_local.isoformat(),
            sections=sections_payload,
        )
        menu_id = save_digest_menu_payload(payload)
        hub = digest_menu_intro_html(today_local.isoformat(), show_listener_note=True)
        await notifier.send_digest_hub(hub, build_digest_menu_keyboard(menu_id, keys))
    else:
        await notifier.send_chunked_parts(head, agenda_blocks, continuation_header=cont)

    await asyncio.sleep(0.35)

    pred_cap = max(1, settings.daily_digest_max_prediction_messages)
    fx_pred = fx if settings.telegram_digest_menu else _prediction_slice(fx, pred_cap, settings)
    if not settings.telegram_digest_menu:
        pred_blocks = [_prediction_block_compact(agenda_idx[r.event_id], r, roll_ctx, e0p, settings) for r in fx_pred]
        pred_head = (
            f"🔮 <b>Pronósticos</b> <code>{esc}</code>\n"
            f"1X2 · goles · tarjetas · corners · mismo orden que la agenda\n"
        )
        pred_cont = f"🔮 <b>Pronósticos (cont.)</b> <code>{esc}</code>\n"
        await notifier.send_chunked_parts(pred_head, pred_blocks, continuation_header=pred_cont)
    for r in fx_pred:
        prov = "espn-free" if r.source == "espn" else "thesportsdb"
        repo.save_fixture_snapshot(r.event_id, prov, r.home_name, r.away_name, {"slug": r.slug, "source": r.source})
        ph, pd_, pa, xh, xa, used_ml, blend_ml_w = _pred_with_optional_e0_ml(r, roll_ctx, e0p, settings)
        for k, p in (("1X2_HOME", ph), ("1X2_DRAW", pd_), ("1X2_AWAY", pa)):
            repo.save_prediction_snapshot(r.event_id, k, p, 0.0, 0.0, xh, xa)
        repo.save_digest_prediction_audit(
            event_id=r.event_id,
            digest_slug=(r.slug or "").strip(),
            local_date_iso=today_local.isoformat(),
            home_team=r.home_name,
            away_team=r.away_name,
            ph=ph,
            pd=pd_,
            pa=pa,
            used_ml=used_ml,
            blend_ml_w=blend_ml_w,
        )
    if not settings.telegram_digest_menu and len(fx) > pred_cap:
        await notifier.send_html(
            f"ℹ️ Pronósticos: {pred_cap}/{len(fx)} · sube <code>DAILY_DIGEST_MAX_PREDICTION_MESSAGES</code> en <code>.env</code>."
        )


def _post_digest_live_eval_worker(settings: Settings, repo: TimeSeriesRepository) -> None:
    from pathlib import Path

    from src.config import repo_root
    from src.predictor.digest_live_evaluation import (
        record_digest_eval_breach_alert,
        run_and_write_digest_live_evaluation,
    )

    root = repo_root()
    try:
        rep = run_and_write_digest_live_evaluation(
            root=root,
            db_path=repo.db_path,
            csv_path=Path(settings.historical_csv_path),
            since_days=settings.digest_live_eval_since_days,
            min_matched=settings.digest_live_eval_min_matched,
            warn_log_loss=settings.digest_live_eval_warn_log_loss,
        )
        record_digest_eval_breach_alert(
            repo,
            rep,
            enabled=settings.digest_live_eval_alert_on_breach,
        )
        LOG.info(
            "digest_live_evaluation | status=%s matched=%s",
            rep.get("status"),
            rep.get("matched"),
        )
    except Exception as exc:
        LOG.warning("digest_live_evaluation: %s", exc)


async def run_prediction_pipeline_free(
    notifier: TelegramNotifier,
    repo: TimeSeriesRepository,
    cfg: PipelineConfig,
) -> None:
    _ = cfg
    settings = get_settings()
    await run_free_daily_digest(notifier, repo, settings)
    if settings.digest_run_live_eval_after_digest:
        await asyncio.to_thread(_post_digest_live_eval_worker, settings, repo)
