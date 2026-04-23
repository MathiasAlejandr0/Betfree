"""Scheduled pipeline jobs and orchestration."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.notifier.telegram_bot import BetAlert, TelegramNotifier
from src.predictor.model_trainer import (
    MatchPrediction,
    TeamStrength,
    calculate_value_edge,
    estimate_expected_goals,
    expected_value_per_unit,
    predict_1x2_probabilities,
)
from src.risk.bankroll import recommended_stake
from src.storage.repository import TimeSeriesRepository


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """Operational thresholds for value bet alerts."""

    bankroll: float = 1000.0
    min_edge: float = 0.03
    min_ev: float = 0.02
    kelly_multiplier: float = 0.25
    max_fraction: float = 0.05
    source_provider: str = "api-football"


def _safe_team_strength() -> TeamStrength:
    """Return baseline strengths until historical feature engineering is added."""
    return TeamStrength(attack_rate=1.0, defense_rate=1.0, elo=1500.0)


def _extract_api_football_odds(odds_payload: list[dict[str, Any]]) -> dict[str, float]:
    """Extract 1X2 odds from API-Football response."""
    result: dict[str, float] = {}
    for item in odds_payload:
        for bookmaker in item.get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                if str(bet.get("name", "")).lower() in {"match winner", "1x2"}:
                    for value in bet.get("values", []):
                        label = str(value.get("value", "")).strip().lower()
                        try:
                            odd = float(value.get("odd"))
                        except (TypeError, ValueError):
                            continue
                        if label == "home":
                            result["home"] = odd
                        elif label in {"draw", "tie"}:
                            result["draw"] = odd
                        elif label == "away":
                            result["away"] = odd
                    return result
    return result


def _select_best_market(
    prediction: MatchPrediction, market_odds: dict[str, float]
) -> tuple[str, float, float, float] | None:
    """Select market with highest expected value."""
    options = {
        "home": prediction.home_win,
        "draw": prediction.draw,
        "away": prediction.away_win,
    }
    best: tuple[str, float, float, float] | None = None
    for market_key, model_probability in options.items():
        odds = market_odds.get(market_key)
        if not odds:
            continue
        edge = calculate_value_edge(model_probability, odds)
        ev = expected_value_per_unit(model_probability, odds)
        row = (market_key, model_probability, edge, ev)
        if best is None or row[3] > best[3]:
            best = row
    return best


async def run_prediction_pipeline_api_football(
    api_client: Any,
    notifier: TelegramNotifier,
    repo: TimeSeriesRepository,
    config: PipelineConfig,
) -> None:
    """Pipeline using API-Football fixtures and odds."""
    fixtures = api_client.get_today_fixtures()
    LOGGER.info("API-Football fixtures: %s", len(fixtures))
    for fixture in fixtures:
        fixture_info = fixture.get("fixture", {})
        fixture_id = fixture_info.get("id")
        if fixture_id is None:
            continue
        teams = fixture.get("teams", {})
        home_name = teams.get("home", {}).get("name", "Home")
        away_name = teams.get("away", {}).get("name", "Away")
        repo.save_fixture_snapshot(
            fixture_id=int(fixture_id),
            source_provider="api-football",
            home_team=home_name,
            away_team=away_name,
            fixture_payload=fixture,
        )

        home_strength = _safe_team_strength()
        away_strength = _safe_team_strength()
        home_xg, away_xg = estimate_expected_goals(home_strength, away_strength)
        prediction = predict_1x2_probabilities(home_xg, away_xg)

        odds_payload = api_client.get_current_odds(fixture_id=int(fixture_id))
        market_odds = _extract_api_football_odds(odds_payload)
        if not market_odds:
            continue
        repo.save_odds_snapshot(fixture_id=int(fixture_id), market_odds=market_odds)

        best = _select_best_market(prediction, market_odds)
        if not best:
            continue
        market_key, model_probability, edge, ev = best
        repo.save_prediction_snapshot(
            fixture_id=int(fixture_id),
            market_key=market_key,
            probability=model_probability,
            edge=edge,
            ev=ev,
            expected_home_goals=home_xg,
            expected_away_goals=away_xg,
        )
        if edge < config.min_edge or ev < config.min_ev:
            continue
        stake = recommended_stake(
            bankroll=config.bankroll,
            win_probability=model_probability,
            decimal_odds=market_odds[market_key],
            kelly_multiplier=config.kelly_multiplier,
            max_fraction=config.max_fraction,
        )
        if stake <= 0:
            continue
        alert = BetAlert(
            match_name=f"{home_name} vs {away_name}",
            suggested_market={"home": "1 (Local)", "draw": "X (Empate)", "away": "2 (Visita)"}[
                market_key
            ],
            probability=model_probability,
            odds=market_odds[market_key],
            stake=stake,
        )
        message = await notifier.send_alert(alert)
        repo.save_alert_sent(
            fixture_id=int(fixture_id),
            market_key=market_key,
            odds_value=market_odds[market_key],
            stake=stake,
            message=message,
        )


async def run_prediction_pipeline_football_data(
    football_data_client: Any,
    notifier: TelegramNotifier,
    repo: TimeSeriesRepository,
    config: PipelineConfig,
) -> None:
    """Pipeline using football-data.org GET /matches endpoint."""
    matches = football_data_client.get_today_matches()
    LOGGER.info("football-data matches: %s", len(matches))
    for match in matches:
        fixture_id = match.get("id")
        if fixture_id is None:
            continue
        home_name = match.get("homeTeam", {}).get("name", "Home")
        away_name = match.get("awayTeam", {}).get("name", "Away")
        repo.save_fixture_snapshot(
            fixture_id=int(fixture_id),
            source_provider="football-data",
            home_team=home_name,
            away_team=away_name,
            fixture_payload=match,
        )
        # football-data /matches free flow often does not include bookmaker odds.
        # We persist fixture snapshots so you can later enrich odds from another source.
        LOGGER.info("Match snapshot saved (football-data): %s vs %s", home_name, away_name)


def build_scheduler(async_job: Any, cron_expression: str = "*/20 * * * *") -> BlockingScheduler:
    """Create blocking scheduler from 5-field cron expression."""
    minute, hour, day, month, day_of_week = cron_expression.split()
    scheduler = BlockingScheduler(timezone="America/Santiago")
    scheduler.add_job(
        async_job,
        trigger=CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
        ),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    return scheduler

