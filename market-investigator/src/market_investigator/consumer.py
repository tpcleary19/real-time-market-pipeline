"""Persist a spike before acknowledging it to Kafka."""

import logging
import os
from pathlib import Path
import signal
from typing import Literal

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
import psycopg
from psycopg.types.json import Jsonb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

log = logging.getLogger(__name__)


class SpikeEvent(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)

    schema_version: Literal[1]
    event_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1, max_length=32)
    direction: Literal["UP", "DOWN"]
    baseline_price: float = Field(gt=0)
    current_price: float = Field(gt=0)
    percentage_change: float
    window_seconds: int = Field(gt=0)
    baseline_timestamp: AwareDatetime
    current_timestamp: AwareDatetime
    detected_at: AwareDatetime


def persist_event(connection, message):
    """Return the validated event after its job is durably committed."""
    event = SpikeEvent.model_validate_json(message.value())
    with connection.transaction():
        connection.execute(
            """INSERT INTO investigation_jobs
               (event_id, symbol, spike, kafka_topic, kafka_partition, kafka_offset)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (event_id) DO NOTHING""",
            (
                event.event_id, event.symbol, Jsonb(event.model_dump(mode="json")),
                message.topic(), message.partition(), message.offset(),
            ),
        )
    return event


def handle_message(connection, consumer, message):
    event = persist_event(connection, message)
    # Synchronous commit also applies to duplicates, whose job already exists.
    offsets = consumer.commit(message=message, asynchronous=False)
    for offset in offsets or []:
        if offset.error is not None:
            raise KafkaException(offset.error)
    log.info("Job recorded event_id=%s symbol=%s", event.event_id, event.symbol)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Match the Rust services' existing root .env, regardless of the current directory.
    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("Set DATABASE_URL in the environment or repository root .env")

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "group.id": os.getenv("INVESTIGATOR_CONSUMER_GROUP", "market-investigator"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })
    try:
        with psycopg.connect(database_url, autocommit=True, connect_timeout=10) as connection:
            # Pooled Neon endpoints reject libpq startup options; set these in-session.
            connection.execute("SET statement_timeout = '15s'")
            connection.execute("SET lock_timeout = '10s'")
            # Fail before reading Kafka if the migration has not been applied.
            connection.execute("SELECT event_id FROM investigation_jobs LIMIT 0")
            topic = os.getenv("SPIKE_OUTPUT_TOPIC", "price-spikes")
            consumer.subscribe([topic])
            log.info("Consuming %s; recording pending investigation jobs", topic)
            while not stopping:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                try:
                    handle_message(connection, consumer, message)
                except Exception as error:
                    # Fail closed: never move past a record that wasn't safely handled.
                    log.error("%s at %s/%s/%s; stopping without advancing past it",
                              type(error).__name__, message.topic(), message.partition(), message.offset())
                    raise SystemExit(1) from None
    except psycopg.errors.UndefinedTable:
        raise SystemExit("Missing investigation_jobs table; apply db/migrations/004_create_investigation_jobs.sql") from None
    except psycopg.Error as error:
        # Error class and SQLSTATE help diagnosis without printing credentials.
        raise SystemExit(
            f"Database operation failed ({type(error).__name__}, SQLSTATE={error.sqlstate or 'none'}); "
            "check connectivity and DATABASE_URL"
        ) from None
    finally:
        consumer.close()
