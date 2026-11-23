from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from threading import Barrier, Event
import unittest
from unittest.mock import Mock, patch
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from market_investigator.investigate import claim_job, run_one, select_job
from market_investigator.scheduler import process_hour, process_tick
from test_intake import EVENT

HOUR = datetime(2026, 11, 23, 14, tzinfo=timezone.utc)


class SchedulerTests(unittest.TestCase):
    def test_backfill_previous_hour_then_live_current_hour(self):
        with patch('market_investigator.scheduler.datetime') as clock:
            clock.now.return_value = HOUR + timedelta(minutes=1)
            with patch('market_investigator.scheduler.process_hour') as process:
                process_tick(Mock(), Mock(), 'model', 'openai', Event())
        self.assertEqual(process.call_args_list[0].args[4], HOUR - timedelta(hours=1))
        self.assertTrue(process.call_args_list[0].kwargs['backfill'])
        self.assertEqual(process.call_args_list[1].args[4], HOUR)
        self.assertNotIn('backfill', process.call_args_list[1].kwargs)

    def test_first_minute_does_not_backfill(self):
        with patch('market_investigator.scheduler.datetime') as clock:
            clock.now.return_value = HOUR + timedelta(seconds=30)
            with patch('market_investigator.scheduler.process_hour') as process:
                process_tick(Mock(), Mock(), 'model', 'openai', Event())
        process.assert_called_once()
        self.assertEqual(process.call_args.args[4], HOUR)

    def test_refreshes_hour_after_slow_backfill(self):
        with patch('market_investigator.scheduler.datetime') as clock:
            clock.now.side_effect = [HOUR + timedelta(minutes=59), HOUR + timedelta(hours=1)]
            with patch('market_investigator.scheduler.process_hour') as process:
                process_tick(Mock(), Mock(), 'model', 'openai', Event())
        self.assertEqual(process.call_args.args[4], HOUR + timedelta(hours=1))

    def test_failures_continue_but_at_most_three_attempts_per_pass(self):
        with patch('market_investigator.scheduler.run_one', side_effect=RuntimeError) as run:
            with self.assertLogs('market_investigator.scheduler', level='ERROR'):
                process_hour(Mock(), Mock(), 'model', 'openai', HOUR, Event(), backfill=True)
        self.assertEqual(run.call_count, 3)
        self.assertTrue(run.call_args.kwargs['backfill'])

    def test_no_candidate_or_full_budget_stops_processing(self):
        with patch('market_investigator.scheduler.run_one', return_value=None) as run:
            process_hour(Mock(), Mock(), 'model', 'openai', HOUR, Event(), backfill=True)
        run.assert_called_once()


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'requires TEST_DATABASE_URL')
class HourlyBudgetDatabaseTests(unittest.TestCase):
    def setUp(self):
        # Concurrent connections share only this test's disposable schema.
        self.schema = 'test_investigator_' + uuid.uuid4().hex
        self.connection = psycopg.connect(
            os.environ['TEST_DATABASE_URL'], autocommit=True, row_factory=dict_row
        )
        self.connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.addCleanup(self.cleanup_schema)
        self.set_search_path(self.connection)
        migrations = Path(__file__).resolve().parents[2] / 'db/migrations'
        for name in ('004_create_investigation_jobs.sql', '005_add_investigation_results.sql',
                     '006_add_investigation_hourly_budget.sql'):
            self.connection.execute((migrations / name).read_text())

    def set_search_path(self, connection):
        connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))

    def cleanup_schema(self):
        try:
            self.connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
        finally:
            self.connection.close()

    def add_job(self, event_id, timestamp):
        spike = EVENT | {'event_id': event_id, 'current_timestamp': timestamp.isoformat()}
        self.connection.execute(
            '''INSERT INTO investigation_jobs
               (event_id, symbol, spike, kafka_topic, kafka_partition, kafka_offset)
               VALUES (%s, 'ZZZZ', %s, 'test', 0, 0)''', (event_id, Jsonb(spike)),
        )

    def count(self):
        return self.connection.execute(
            'SELECT started_count FROM investigation_hourly_budget WHERE hour_start = %s',
            (HOUR,),
        ).fetchone()['started_count']

    def test_live_selection_excludes_backlog_and_preview_matches(self):
        self.add_job('old', HOUR - timedelta(days=3))
        self.add_job('new', HOUR + timedelta(minutes=5))
        with patch('market_investigator.investigate.datetime') as clock:
            clock.now.return_value = HOUR
            self.assertEqual(select_job(self.connection)['event_id'], 'new')
        job, _ = claim_job(self.connection, hour_start=HOUR)
        self.assertEqual(job['event_id'], 'new')
        self.assertEqual(claim_job(self.connection, hour_start=HOUR), (None, None))
        self.assertEqual(self.count(), 1)

    def test_backfill_oldest_first_and_only_remaining_slot(self):
        self.connection.execute('INSERT INTO investigation_hourly_budget VALUES (%s, 2)', (HOUR,))
        self.add_job('recent', HOUR - timedelta(days=1))
        self.add_job('oldest', HOUR - timedelta(days=3))
        job, _ = claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.assertEqual(job['event_id'], 'oldest')
        self.assertEqual(self.count(), 3)
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        self.assertEqual(self.connection.execute(
            "SELECT budget_hour FROM investigation_jobs WHERE event_id = 'oldest'"
        ).fetchone()['budget_hour'], HOUR)

    def test_no_eligible_job_does_not_consume_budget(self):
        self.add_job('future', HOUR + timedelta(hours=1))
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        self.assertEqual(self.count(), 0)

    def test_failure_keeps_slot_and_does_not_retry_failed_job(self):
        self.add_job('old', HOUR - timedelta(days=3))
        with patch('market_investigator.investigate.lookup_asset', side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                run_one(self.connection, Mock(), 'model', hour_start=HOUR, backfill=True)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.connection.execute(
            "SELECT status FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()['status'], 'failed')
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        self.assertEqual(self.count(), 1)

    def test_concurrent_workers_share_budget_and_restarts_preserve_it(self):
        for i in range(8):
            self.add_job(str(i), HOUR - timedelta(days=1, minutes=i))
        barrier = Barrier(8)

        def claim(_):
            with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True,
                                 row_factory=dict_row) as connection:
                self.set_search_path(connection)
                barrier.wait(timeout=10)
                job, _ = claim_job(connection, hour_start=HOUR, backfill=True)
                return job['event_id'] if job else None

        with ThreadPoolExecutor(max_workers=8) as pool:
            claimed = [event_id for event_id in pool.map(claim, range(8)) if event_id]
        self.assertEqual(len(claimed), 3)
        self.assertEqual(len(set(claimed)), 3)
        self.assertEqual(self.count(), 3)
        with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True,
                             row_factory=dict_row) as connection:
            self.set_search_path(connection)
            self.assertEqual(claim_job(connection, hour_start=HOUR, backfill=True), (None, None))
