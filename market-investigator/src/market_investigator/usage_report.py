"""Read-only usage, estimated spending, hourly slots, and backlog report."""

import argparse
from datetime import timedelta
import json
import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg
from psycopg.rows import DictRow, dict_row


def get_usage_report(connection, days):
    daily = connection.execute(
        """SELECT (started_at AT TIME ZONE 'UTC')::date AS day_utc,
                  count(*) AS attempts,
                  count(*) FILTER (WHERE outcome = 'completed') AS completed,
                  count(*) FILTER (WHERE outcome = 'skipped') AS skipped,
                  count(*) FILTER (WHERE outcome = 'failed') AS failed,
                  count(*) FILTER (WHERE outcome = 'expired') AS expired,
                  count(*) FILTER (WHERE outcome = 'running') AS running,
                  count(*) FILTER (WHERE trigger = 'backfill') AS backfill_attempts,
                  sum(input_tokens) AS reported_input_tokens,
                  sum(output_tokens) AS reported_output_tokens,
                  sum(cache_read_tokens) AS reported_cache_read_tokens,
                  sum(cache_write_tokens) AS reported_cache_write_tokens,
                  sum(billable_searches) AS reported_searches,
                  count(*) FILTER (WHERE usage_status = 'unknown') AS unknown_usage_attempts,
                  count(*) FILTER (WHERE billable_searches IS NULL) AS unknown_search_attempts,
                  sum(estimated_cost_usd) AS known_estimated_cost_usd,
                  count(*) FILTER (WHERE cost_status = 'unknown') AS unknown_cost_attempts,
                  CASE WHEN count(*) FILTER (WHERE cost_status = 'unknown') = 0
                    THEN sum(estimated_cost_usd) /
                         nullif(count(*) FILTER (WHERE outcome = 'completed'), 0)
                    ELSE NULL END AS estimated_cost_per_completed_report_usd,
                  avg(duration_ms) AS avg_observed_duration_ms,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY model_duration_ms)
                      AS p95_model_duration_ms
           FROM investigation_attempts
           WHERE started_at >= (date_trunc('day', now() AT TIME ZONE 'UTC')
                                AT TIME ZONE 'UTC') - %s
           GROUP BY 1 ORDER BY 1 DESC""",
        (timedelta(days=days - 1),),
    ).fetchall()
    by_model = connection.execute(
        """SELECT provider, requested_model, response_model, outcome, count(*) AS attempts,
                  sum(estimated_cost_usd) AS known_estimated_cost_usd,
                  count(*) FILTER (WHERE cost_status = 'unknown') AS unknown_cost_attempts
           FROM investigation_attempts WHERE started_at >=
               (date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') - %s
           GROUP BY provider, requested_model, response_model, outcome
           ORDER BY attempts DESC""", (timedelta(days=days - 1),),
    ).fetchall()
    budgets = connection.execute(
        """SELECT b.hour_start, b.started_count, 3 - b.started_count AS unused_slots,
                  count(a.claim_token) FILTER (WHERE a.trigger = 'backfill') AS tracked_backfill_attempts,
                  count(a.claim_token) AS tracked_attempts
           FROM investigation_hourly_budget AS b
           LEFT JOIN investigation_attempts AS a ON a.budget_hour = b.hour_start
           WHERE b.hour_start >= now() - %s
           GROUP BY b.hour_start, b.started_count ORDER BY b.hour_start DESC""",
        (timedelta(days=days),),
    ).fetchall()
    backlog = connection.execute(
        """SELECT count(*) FILTER (WHERE status = 'pending') AS pending,
                  count(*) FILTER (WHERE status = 'pending' AND attempts < 3) AS eligible_pending,
                  count(*) FILTER (WHERE status = 'running') AS running,
                  min((spike->>'current_timestamp')::timestamptz)
                      FILTER (WHERE status = 'pending') AS oldest_pending_event_time,
                  min(created_at) FILTER (WHERE status = 'pending') AS oldest_pending_intake_time
           FROM investigation_jobs""",
    ).fetchone()
    return {"daily": daily, "by_model": by_model, "hourly_budgets": budgets, "backlog": backlog}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days', type=int, default=7, help='UTC days to report (default: 7)')
    args = parser.parse_args()
    if not 1 <= args.days <= 365:
        parser.error('--days must be between 1 and 365')
    load_dotenv(Path(__file__).resolve().parents[3] / '.env')
    if not os.getenv('DATABASE_URL'):
        parser.error('Set DATABASE_URL in the repository root .env')
    try:
        with psycopg.Connection[DictRow].connect(
            os.environ['DATABASE_URL'], row_factory=dict_row, connect_timeout=10,
        ) as connection:
            connection.read_only = True
            connection.execute("SET LOCAL statement_timeout = '15s'")
            report = get_usage_report(connection, args.days)
        print(json.dumps(report, indent=2, default=str))
    except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
        raise SystemExit('Apply migrations 004 through 007 before viewing usage') from None
    except psycopg.Error as error:
        raise SystemExit(f'Usage report failed ({type(error).__name__})') from None


if __name__ == '__main__':
    main()
