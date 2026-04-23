"""Application entrypoint for Betfree ecosystem."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.config import get_settings, validate_provider_token
from src.data_engine.api_client import build_default_api_football_client
from src.data_engine.football_data_client import build_default_football_data_client
from src.notifier.telegram_bot import build_default_notifier
from src.scheduler.jobs import (
    PipelineConfig,
    build_scheduler,
    run_prediction_pipeline_api_football,
    run_prediction_pipeline_football_data,
)
from src.storage.database import init_db
from src.storage.repository import TimeSeriesRepository


def parse_args() -> argparse.Namespace:
    """Parse CLI args for run mode and provider selection."""
    parser = argparse.ArgumentParser(description="Betfree Prediction Engine")
    parser.add_argument("--run-once", action="store_true", help="Run once and exit.")
    parser.add_argument("--cron", default="*/20 * * * *", help="5-field cron expression.")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Bankroll amount.")
    parser.add_argument(
        "--provider",
        choices=["api-football", "football-data"],
        default="football-data",
        help="Data provider for fixture ingestion.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    """Configure baseline logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    """Initialize dependencies and run selected mode."""
    configure_logging()
    args = parse_args()
    settings = get_settings()
    validate_provider_token(settings, args.provider)
    init_db(settings.sqlite_db_path)
    repo = TimeSeriesRepository(settings.sqlite_db_path)
    notifier = build_default_notifier()
    config = PipelineConfig(bankroll=args.bankroll, source_provider=args.provider)

    if args.provider == "api-football":
        api_client = build_default_api_football_client()
        async_job = lambda: run_prediction_pipeline_api_football(api_client, notifier, repo, config)
    else:
        football_data_client = build_default_football_data_client()
        async_job = lambda: run_prediction_pipeline_football_data(
            football_data_client, notifier, repo, config
        )

    if args.run_once:
        asyncio.run(async_job())
        return

    scheduler = build_scheduler(async_job=lambda: asyncio.run(async_job()), cron_expression=args.cron)
    logging.getLogger(__name__).info(
        "Scheduler started | provider=%s | cron=%s", args.provider, args.cron
    )
    scheduler.start()


if __name__ == "__main__":
    main()

