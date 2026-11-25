"""Process new spikes and fill unused hourly slots from the oldest backlog."""

from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import signal
from threading import Event

from anthropic import Anthropic
from dotenv import load_dotenv
from openai import OpenAI
import psycopg
from psycopg.rows import DictRow, dict_row

from market_investigator.investigate import provider_settings, run_one

log = logging.getLogger(__name__)


def process_hour(
    connection, client, model, provider, hour_start, stopping, *, backfill=False
):
    # claim_job enforces the durable limit, including earlier attempts.
    for _ in range(3):
        if stopping.is_set():
            return

        try:
            result = run_one(
                connection,
                client,
                model,
                provider=provider,
                hour_start=hour_start,
                backfill=backfill,
                trigger="backfill" if backfill else "current_hour",
            )
        except psycopg.Error:
            # Let the outer loop close the connection and reconnect.
            raise
        except Exception as error:
            # The failed attempt keeps its reserved slot.
            log.error(
                "Investigation failed hour=%s error=%s",
                hour_start.isoformat(),
                type(error).__name__,
            )
            continue

        if result is None:
            # Budget exhausted or no pending candidates.
            return

        log.info("Investigation completed hour=%s", hour_start.isoformat())


def process_tick(connection, client, model, provider, stopping):
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    # Fill unused slots from the last closed hour with older pending work.
    if now >= current_hour + timedelta(minutes=1):
        process_hour(
            connection, client, model, provider,
            current_hour - timedelta(hours=1), stopping, backfill=True,
        )

    # Research can take us across an hour boundary; use a fresh clock reading.
    current_hour = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    process_hour(connection, client, model, provider, current_hour, stopping)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    load_dotenv(Path(__file__).resolve().parents[3] / ".env")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("Set DATABASE_URL in the repository root .env")

    provider, key_name, model = provider_settings()
    api_key = os.getenv(key_name)
    if not api_key:
        raise SystemExit(f"Set {key_name} in the repository root .env")

    stopping = Event()

    def stop(signum, frame):
        stopping.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    client_class = Anthropic if provider == "anthropic" else OpenAI

    with client_class(
        api_key=api_key,
        timeout=120.0,
        max_retries=0,
    ) as client:
        log.info("Scheduler started provider=%s model=%s", provider, model)

        while not stopping.is_set():
            try:
                with psycopg.Connection[DictRow].connect(
                    database_url,
                    autocommit=True,
                    connect_timeout=10,
                    row_factory=dict_row,
                ) as connection:
                    connection.execute("SET statement_timeout = '15s'")
                    connection.execute("SET lock_timeout = '10s'")

                    process_tick(connection, client, model, provider, stopping)

            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                raise SystemExit("Apply db/migrations/004 through 007 before starting the scheduler") from None
            except psycopg.Error as error:
                log.error(
                    "Database error; will reconnect: %s",
                    type(error).__name__,
                )

            # Ctrl-C interrupts the wait immediately.
            stopping.wait(30)

    log.info("Scheduler stopped")


if __name__ == "__main__":
    main()
