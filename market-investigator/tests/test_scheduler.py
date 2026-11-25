from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
import subprocess
import sys
from pathlib import Path
from threading import Barrier, Event
import unittest
from unittest.mock import Mock, patch
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from market_investigator.investigate import claim_job, recover_expired_jobs, run_one, select_job
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
                     '006_add_investigation_hourly_budget.sql', '007_create_investigation_attempts.sql'):
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


    def expire_job(self, event_id):
        self.connection.execute(
            "UPDATE investigation_jobs SET started_at = now() - interval '11 minutes' WHERE event_id = %s",
            (event_id,),
        )

    def test_recent_claim_is_not_recovered(self):
        self.add_job('active', HOUR - timedelta(days=1))
        job, token = claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.assertEqual(recover_expired_jobs(self.connection), [])
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        self.assertEqual(self.count(), 1)
        row = self.connection.execute(
            "SELECT claim_token FROM investigation_jobs WHERE event_id = 'active'"
        ).fetchone()
        self.assertEqual(row['claim_token'], token)

    def test_expired_claim_retries_in_new_budget_without_refunding_old_budget(self):
        self.add_job('old', HOUR - timedelta(days=1))
        _, old_token = claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.expire_job('old')
        next_hour = HOUR + timedelta(hours=1)
        job, new_token = claim_job(self.connection, hour_start=next_hour, backfill=True)
        self.assertEqual(job['event_id'], 'old')
        self.assertNotEqual(old_token, new_token)
        row = self.connection.execute(
            "SELECT attempts, budget_hour FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()
        self.assertEqual(row, {'attempts': 2, 'budget_hour': next_hour})
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.connection.execute(
            'SELECT started_count FROM investigation_hourly_budget WHERE hour_start = %s',
            (next_hour,),
        ).fetchone()['started_count'], 1)

    def test_expiration_stops_after_three_attempts(self):
        self.add_job('old', HOUR - timedelta(days=1))
        for _ in range(3):
            job, _ = claim_job(self.connection, hour_start=HOUR, backfill=True)
            self.assertIsNotNone(job)
            self.expire_job('old')
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        row = self.connection.execute(
            "SELECT status, claim_token, last_error, attempts FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()
        self.assertEqual(row, {'status': 'failed', 'claim_token': None,
                               'last_error': 'ClaimExpiredAttemptLimit', 'attempts': 3})
        self.assertEqual(self.count(), 3)

    def test_recovery_commits_even_when_budget_is_full(self):
        self.add_job('old', HOUR - timedelta(days=1))
        claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.expire_job('old')
        self.connection.execute('UPDATE investigation_hourly_budget SET started_count = 3')
        self.assertEqual(claim_job(self.connection, hour_start=HOUR, backfill=True), (None, None))
        row = self.connection.execute(
            "SELECT status, claim_token, attempts FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()
        self.assertEqual(row, {'status': 'pending', 'claim_token': None, 'attempts': 1})
        self.assertEqual(self.count(), 3)

    def test_old_worker_cannot_save_or_fail_reclaimed_job(self):
        self.add_job('old', HOUR - timedelta(days=1))
        new_token = None

        def reclaim_during_report(*args, **kwargs):
            nonlocal new_token
            self.expire_job('old')
            _, new_token = claim_job(self.connection, hour_start=HOUR, backfill=True)
            return {'report': 'obsolete report'}

        with patch('market_investigator.investigate.lookup_asset', return_value={'outcome': 'verified'}):
            with patch('market_investigator.investigate.request_report', side_effect=reclaim_during_report):
                with self.assertRaisesRegex(RuntimeError, 'claim expired or changed'):
                    run_one(self.connection, Mock(), 'model', hour_start=HOUR, backfill=True)
        row = self.connection.execute(
            "SELECT status, claim_token, result, last_error FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()
        self.assertEqual(row, {'status': 'running', 'claim_token': new_token,
                               'result': None, 'last_error': None})
        self.assertEqual(self.count(), 2)

    def test_process_exit_after_claim_is_recovered(self):
        self.add_job('crash', HOUR - timedelta(days=1))
        script = """
import os
import sys
from datetime import datetime
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from market_investigator.investigate import claim_job
with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True, row_factory=dict_row) as connection:
    connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(sys.argv[1])))
    job, _ = claim_job(connection, hour_start=datetime.fromisoformat(sys.argv[2]), backfill=True)
    assert job is not None
    os._exit(17)
"""
        child = subprocess.run([sys.executable, '-c', script, self.schema, HOUR.isoformat()],
                               capture_output=True, text=True, timeout=15)
        self.assertEqual(child.returncode, 17, child.stderr)
        self.assertEqual(self.count(), 1)
        self.expire_job('crash')
        job, _ = claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.assertEqual(job['event_id'], 'crash')
        self.assertEqual(self.count(), 2)

    def test_exhausted_pending_job_is_not_claimed_or_previewed(self):
        self.add_job('exhausted', HOUR + timedelta(minutes=1))
        self.connection.execute('UPDATE investigation_jobs SET attempts = 3')
        with patch('market_investigator.investigate.datetime') as clock:
            clock.now.return_value = HOUR
            self.assertIsNone(select_job(self.connection))
        self.assertEqual(claim_job(self.connection, hour_start=HOUR), (None, None))
        self.assertEqual(self.count(), 0)

    def test_concurrent_recovery_claims_expired_job_once(self):
        self.add_job('crash', HOUR - timedelta(days=1))
        claim_job(self.connection, hour_start=HOUR, backfill=True)
        self.expire_job('crash')
        barrier = Barrier(2)

        def recover_and_claim(_):
            with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True,
                                 row_factory=dict_row) as connection:
                self.set_search_path(connection)
                barrier.wait(timeout=10)
                job, _ = claim_job(connection, hour_start=HOUR, backfill=True)
                return job['event_id'] if job else None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(recover_and_claim, range(2)))
        self.assertEqual(results.count('crash'), 1)
        self.assertEqual(results.count(None), 1)
        self.assertEqual(self.count(), 2)

    def usage_attempt(self, token):
        return self.connection.execute(
            'SELECT * FROM investigation_attempts WHERE claim_token = %s', (token,),
        ).fetchone()

    def claude_client(self, stop_reason='end_turn'):
        from types import SimpleNamespace
        usage = {'input_tokens': 1000, 'output_tokens': 200,
                 'cache_read_input_tokens': 0, 'cache_creation_input_tokens': 0,
                 'server_tool_use': {'web_search_requests': 2}}
        client = Mock()
        client.messages.create.return_value = SimpleNamespace(
            id='msg_usage_test', model='claude-sonnet-5-5', stop_reason=stop_reason,
            usage=Mock(model_dump=Mock(return_value=usage)),
            content=[Mock(model_dump=Mock(return_value={'type': 'text', 'text': 'Research with citations'}))],
        )
        return client

    def test_claim_creates_ledger_atomically_with_budget_and_job(self):
        self.add_job('old', HOUR - timedelta(days=1))
        with self.assertRaises(psycopg.errors.CheckViolation):
            claim_job(self.connection, hour_start=HOUR, backfill=True, trigger='invalid')
        self.assertEqual(self.connection.execute(
            "SELECT status, attempts FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone(), {'status': 'pending', 'attempts': 0})
        self.assertEqual(self.connection.execute(
            'SELECT count(*) AS n FROM investigation_hourly_budget'
        ).fetchone()['n'], 0)
        job, token = claim_job(self.connection, hour_start=HOUR, backfill=True,
                               provider='anthropic', model='claude-sonnet-5-5')
        attempt = self.usage_attempt(token)
        self.assertEqual(attempt['attempt_number'], 1)
        self.assertEqual(attempt['trigger'], 'backfill')
        self.assertEqual(attempt['outcome'], 'running')
        self.assertEqual(attempt['pricing_snapshot']['version'], '2026-11-23-standard-v1')
        self.assertIsNone(attempt['estimated_cost_usd'])
        self.assertEqual(self.count(), 1)

    def test_completed_report_records_tokens_searches_cost_and_latency(self):
        from decimal import Decimal
        self.add_job('old', HOUR - timedelta(days=1))
        with patch('market_investigator.investigate.lookup_asset', return_value={'outcome': 'verified'}):
            result = run_one(self.connection, self.claude_client(), 'claude-sonnet-5-5',
                             provider='anthropic', hour_start=HOUR, backfill=True, trigger='backfill')
        row = self.connection.execute('SELECT * FROM investigation_attempts').fetchone()
        self.assertEqual(row['outcome'], 'completed')
        self.assertEqual(row['usage_status'], 'reported')
        self.assertEqual(row['raw_usage'], result['usage'])
        self.assertEqual(row['billable_searches'], 2)
        self.assertEqual(row['estimated_cost_usd'], Decimal('0.024'))
        self.assertEqual(row['cost_status'], 'estimated')
        self.assertGreaterEqual(row['duration_ms'], 0)
        self.assertGreaterEqual(row['model_duration_ms'], 0)
        self.assertEqual(row['response_id'], 'msg_usage_test')

    def test_incomplete_report_is_failed_but_usage_is_preserved(self):
        self.add_job('old', HOUR - timedelta(days=1))
        with patch('market_investigator.investigate.lookup_asset', return_value={'outcome': 'verified'}):
            with self.assertRaisesRegex(RuntimeError, 'did not complete'):
                run_one(self.connection, self.claude_client('max_tokens'), 'claude-sonnet-5-5',
                        provider='anthropic', hour_start=HOUR, backfill=True)
        row = self.connection.execute('SELECT * FROM investigation_attempts').fetchone()
        self.assertEqual(row['outcome'], 'failed')
        self.assertEqual(row['usage_status'], 'reported')
        self.assertEqual(row['output_tokens'], 200)
        self.assertGreater(row['estimated_cost_usd'], 0)
        self.assertEqual(row['error_class'], 'RuntimeError')

    def test_asset_skip_is_zero_cost_without_a_model_request(self):
        self.add_job('old', HOUR - timedelta(days=1))
        with patch('market_investigator.investigate.lookup_asset', return_value={'outcome': 'not_found'}):
            run_one(self.connection, Mock(), 'claude-sonnet-5-5', provider='anthropic',
                    hour_start=HOUR, backfill=True)
        row = self.connection.execute('SELECT * FROM investigation_attempts').fetchone()
        self.assertEqual(row['outcome'], 'skipped')
        self.assertEqual(row['cost_status'], 'not_requested')
        self.assertEqual(row['estimated_cost_usd'], 0)
        self.assertEqual(row['input_tokens'], 0)
        self.assertIsNone(row['model_request_started_at'])

    def test_provider_timeout_records_unknown_cost_not_zero(self):
        self.add_job('old', HOUR - timedelta(days=1))
        client = Mock()
        client.messages.create.side_effect = TimeoutError('must not be stored')
        with patch('market_investigator.investigate.lookup_asset', return_value={'outcome': 'verified'}):
            with self.assertRaises(TimeoutError):
                run_one(self.connection, client, 'claude-sonnet-5-5', provider='anthropic',
                        hour_start=HOUR, backfill=True)
        row = self.connection.execute('SELECT * FROM investigation_attempts').fetchone()
        self.assertEqual(row['outcome'], 'failed')
        self.assertEqual(row['error_class'], 'TimeoutError')
        self.assertIsNone(row['estimated_cost_usd'])
        self.assertIsNone(row['input_tokens'])
        self.assertEqual(row['cost_unknown_reason'], 'no_provider_response')

    def test_crash_after_model_start_keeps_unknown_usage_on_expired_attempt(self):
        from market_investigator.usage import begin_model_request
        self.add_job('old', HOUR - timedelta(days=1))
        _, old_token = claim_job(self.connection, hour_start=HOUR, backfill=True,
                                 provider='anthropic', model='claude-sonnet-5-5')
        begin_model_request(self.connection, old_token)
        self.expire_job('old')
        _, new_token = claim_job(self.connection, hour_start=HOUR, backfill=True)
        row = self.usage_attempt(old_token)
        self.assertEqual(row['outcome'], 'expired')
        self.assertEqual(row['cost_status'], 'unknown')
        self.assertIsNone(row['estimated_cost_usd'])
        self.assertIsNone(row['duration_ms'])
        self.assertEqual(self.usage_attempt(new_token)['attempt_number'], 2)
        self.assertEqual(self.count(), 2)

    def test_late_response_updates_only_old_attempt_usage_not_its_outcome(self):
        from market_investigator.usage import begin_model_request, record_response
        self.add_job('old', HOUR - timedelta(days=1))
        _, old_token = claim_job(self.connection, hour_start=HOUR, backfill=True,
                                 provider='anthropic', model='claude-sonnet-5-5')
        begin_model_request(self.connection, old_token)
        self.expire_job('old')
        _, new_token = claim_job(self.connection, hour_start=HOUR, backfill=True)
        usage = self.claude_client().messages.create.return_value.usage.model_dump()
        record_response(self.connection, old_token,
                        {'model': 'claude-sonnet-5-5', 'response_id': 'late', 'usage': usage})
        self.assertEqual(self.usage_attempt(old_token)['outcome'], 'expired')
        self.assertEqual(self.usage_attempt(old_token)['billable_searches'], 2)
        self.assertEqual(self.usage_attempt(new_token)['outcome'], 'running')
        self.assertIsNone(self.usage_attempt(new_token)['raw_usage'])
        self.assertEqual(self.connection.execute(
            "SELECT claim_token FROM investigation_jobs WHERE event_id = 'old'"
        ).fetchone()['claim_token'], new_token)

    def test_usage_report_exposes_unknown_cost_and_backlog(self):
        from market_investigator.usage_report import get_usage_report
        self.add_job('old', HOUR - timedelta(days=1))
        claim_job(self.connection, hour_start=HOUR, backfill=True)
        report = get_usage_report(self.connection, 7)
        self.assertEqual(report['daily'][0]['attempts'], 1)
        self.assertEqual(report['daily'][0]['unknown_cost_attempts'], 1)
        self.assertIsNone(report['daily'][0]['estimated_cost_per_completed_report_usd'])
        self.assertEqual(report['backlog']['running'], 1)

    def test_response_capture_is_idempotent_and_uses_saved_prices(self):
        from decimal import Decimal
        from market_investigator.usage import begin_model_request, record_response
        self.add_job('old', HOUR - timedelta(days=1))
        _, token = claim_job(self.connection, hour_start=HOUR, backfill=True,
                              provider='anthropic', model='claude-sonnet-5-5')
        begin_model_request(self.connection, token)
        usage = self.claude_client().messages.create.return_value.usage.model_dump()
        response = {'model': 'claude-sonnet-5-5', 'response_id': 'first', 'usage': usage}
        with patch('market_investigator.usage.PRICES', {}):
            record_response(self.connection, token, response)
        record_response(self.connection, token, response | {'response_id': 'duplicate', 'usage': {}})
        row = self.usage_attempt(token)
        self.assertEqual(row['estimated_cost_usd'], Decimal('0.024'))
        self.assertEqual(row['response_id'], 'first')
        self.assertEqual(row['input_tokens'], 1000)

    def test_nonstandard_service_tier_is_not_silently_priced_as_standard(self):
        from market_investigator.usage import begin_model_request, record_response
        self.add_job('old', HOUR - timedelta(days=1))
        _, token = claim_job(self.connection, hour_start=HOUR, backfill=True,
                              provider='anthropic', model='claude-sonnet-5-5')
        begin_model_request(self.connection, token)
        usage = self.claude_client().messages.create.return_value.usage.model_dump()
        record_response(self.connection, token,
                        {'model': 'claude-sonnet-5-5', 'response_id': 'tier',
                         'usage': usage, 'service_tier': 'priority'})
        row = self.usage_attempt(token)
        self.assertIsNone(row['estimated_cost_usd'])
        self.assertEqual(row['cost_unknown_reason'], 'unsupported_service_tier')
        self.assertEqual(row['response_service_tier'], 'priority')
