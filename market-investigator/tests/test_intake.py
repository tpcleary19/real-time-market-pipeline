import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from pydantic import ValidationError

from market_investigator.consumer import SpikeEvent, handle_message, persist_event


EVENT = {
    "schema_version": 1, "event_id": "ZZZZ-UP-2-123", "symbol": "ZZZZ",
    "direction": "UP", "baseline_price": 100, "current_price": 110,
    "percentage_change": 10, "window_seconds": 600,
    "baseline_timestamp": "2026-11-20T15:00:00Z",
    "current_timestamp": "2026-11-20T15:00:01Z",
    "detected_at": "2026-11-20T15:00:01Z",
}


def message():
    return Mock(value=Mock(return_value=json.dumps(EVENT).encode()),
                topic=Mock(return_value="price-spikes"),
                partition=Mock(return_value=0), offset=Mock(return_value=5))


class IntakeTests(unittest.TestCase):
    def test_commit_only_after_persistence_returns(self):
        operations = []
        consumer = Mock()
        consumer.commit.side_effect = lambda **kw: operations.append("kafka") or []
        with patch("market_investigator.consumer.persist_event") as persist:
            persist.side_effect = lambda *args: operations.append("database") or SpikeEvent(**EVENT)
            handle_message(Mock(), consumer, message())
        self.assertEqual(operations, ["database", "kafka"])
        self.assertFalse(consumer.commit.call_args.kwargs["asynchronous"])

    def test_database_failure_does_not_commit_kafka(self):
        consumer = Mock()
        with patch("market_investigator.consumer.persist_event", side_effect=RuntimeError("db down")):
            with self.assertRaises(RuntimeError):
                handle_message(Mock(), consumer, message())
        consumer.commit.assert_not_called()

    def test_database_commit_failure_does_not_commit_kafka(self):
        from unittest.mock import MagicMock
        connection, consumer = MagicMock(), Mock()
        connection.transaction.return_value.__exit__.side_effect = RuntimeError("commit failed")
        with self.assertRaises(RuntimeError):
            handle_message(connection, consumer, message())
        connection.execute.assert_called_once()
        consumer.commit.assert_not_called()

    def test_invalid_or_unknown_schema_does_not_touch_database_or_kafka(self):
        for changed in ({"schema_version": 2}, {"current_price": 0},
                        {"percentage_change": float("nan")},
                        {"detected_at": "2026-11-20T15:00:00"}):
            connection, consumer, record = Mock(), Mock(), message()
            record.value.return_value = json.dumps(EVENT | changed).encode()
            with self.assertRaises(ValidationError):
                handle_message(connection, consumer, record)
            connection.execute.assert_not_called()
            consumer.commit.assert_not_called()

    def test_partition_commit_error_is_a_failure(self):
        consumer = Mock()
        consumer.commit.return_value = [Mock(error="commit rejected")]
        with patch("market_investigator.consumer.persist_event", return_value=SpikeEvent(**EVENT)):
            with self.assertRaises(Exception):
                handle_message(Mock(), consumer, message())


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "requires a separate TEST_DATABASE_URL")
class DatabaseTests(unittest.TestCase):
    def test_replay_creates_one_job_without_resetting_completed_work(self):
        import psycopg
        migration = Path(__file__).resolve().parents[2] / "db/migrations/004_create_investigation_jobs.sql"
        with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as connection:
            # Session-local table; no changes to live tables or migrations.
            connection.execute(migration.read_text().replace(
                "CREATE TABLE IF NOT EXISTS investigation_jobs", "CREATE TEMP TABLE investigation_jobs"))
            persist_event(connection, message())
            connection.execute("UPDATE investigation_jobs SET status = 'completed'")
            persist_event(connection, message())
            rows = connection.execute("SELECT event_id, status, spike FROM investigation_jobs").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][1], "completed")
            self.assertEqual(rows[0][2]["symbol"], "ZZZZ")
