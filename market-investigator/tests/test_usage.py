from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from market_investigator.investigate import request_report
from market_investigator.usage import estimate_cost, normalize_usage, pricing_snapshot
from test_intake import EVENT


class UsageTests(unittest.TestCase):
    def test_claude_cache_and_search_costs_do_not_double_count_input(self):
        usage = {'input_tokens': 1000, 'output_tokens': 200,
                 'cache_read_input_tokens': 300, 'cache_creation_input_tokens': 150,
                 'cache_creation': {'ephemeral_5m_input_tokens': 100, 'ephemeral_1h_input_tokens': 50},
                 'server_tool_use': {'web_search_requests': 2}}
        metrics = normalize_usage('anthropic', usage, None)
        price, reason = estimate_cost('anthropic', 'claude-sonnet-5-5', usage, metrics,
                                     pricing_snapshot('anthropic', 'claude-sonnet-5-5'))
        self.assertIsNone(reason)
        self.assertEqual(price, Decimal('0.02448'))
        self.assertEqual(metrics['billable_searches'], 2)

    def test_openai_cached_and_reasoning_tokens_are_not_double_counted(self):
        usage = {'input_tokens': 1000, 'output_tokens': 200,
                 'input_tokens_details': {'cached_tokens': 300, 'cache_write_tokens': 0},
                 'output_tokens_details': {'reasoning_tokens': 100}}
        metrics = normalize_usage('openai', usage, {'web_search': {'num_requests': 2}})
        price, reason = estimate_cost('openai', 'gpt-5-mini-2025-08-07', usage, metrics,
                                     pricing_snapshot('openai', 'gpt-5-mini'))
        self.assertIsNone(reason)
        self.assertEqual(price, Decimal('0.0205825'))
        self.assertEqual(metrics['reasoning_tokens'], 100)

    def test_missing_usage_or_search_count_does_not_become_zero_cost(self):
        usage = {'input_tokens': 1000, 'output_tokens': 200, 'cache_read_input_tokens': 0,
                 'cache_creation_input_tokens': 0}
        metrics = normalize_usage('anthropic', usage, None)
        price, reason = estimate_cost('anthropic', 'claude-sonnet-5-5', usage, metrics,
                                     pricing_snapshot('anthropic', 'claude-sonnet-5-5'))
        self.assertIsNone(metrics['billable_searches'])
        self.assertIsNone(price)
        self.assertEqual(reason, 'missing_usage')
        self.assertTrue(all(value is None for value in normalize_usage('openai', None, None).values()))

    def test_unknown_models_and_cache_ttls_are_not_guessed(self):
        self.assertIsNone(pricing_snapshot('anthropic', 'future-model'))
        usage = {'input_tokens': 1, 'output_tokens': 1, 'cache_read_input_tokens': 0,
                 'cache_creation_input_tokens': 100, 'server_tool_use': {'web_search_requests': 0}}
        price, reason = estimate_cost('anthropic', 'claude-sonnet-5-5', usage,
                                     normalize_usage('anthropic', usage, None),
                                     pricing_snapshot('anthropic', 'claude-sonnet-5-5'))
        self.assertIsNone(price)
        self.assertEqual(reason, 'missing_cache_ttl')

    def test_openai_tool_blocks_are_not_used_as_billable_search_counts(self):
        metrics = normalize_usage('openai', {'input_tokens': 1, 'output_tokens': 1}, None)
        self.assertIsNone(metrics['billable_searches'])

    def test_incomplete_claude_response_reports_usage_before_raising(self):
        client = Mock()
        client.messages.create.return_value = SimpleNamespace(
            stop_reason='max_tokens', model='claude-sonnet-5-5', id='msg_partial',
            usage=Mock(model_dump=Mock(return_value={'input_tokens': 10, 'output_tokens': 5000})),
        )
        capture = Mock()
        with self.assertRaisesRegex(RuntimeError, 'did not complete'):
            request_report(client, 'claude-sonnet-5-5', EVENT, provider='anthropic', on_response=capture)
        self.assertEqual(capture.call_args.args[0]['usage']['output_tokens'], 5000)
        self.assertEqual(capture.call_args.args[0]['response_id'], 'msg_partial')

    def test_incomplete_openai_response_preserves_reported_tool_usage(self):
        client = Mock()
        client.responses.create.return_value = SimpleNamespace(
            status='incomplete', output_text='', model='gpt-5-mini', id='resp_partial',
            usage=Mock(model_dump=Mock(return_value={'input_tokens': 10, 'output_tokens': 5000})),
            tool_usage={'web_search': {'num_requests': 2}},
        )
        capture = Mock()
        with self.assertRaisesRegex(RuntimeError, 'did not complete'):
            request_report(client, 'gpt-5-mini', EVENT, on_response=capture)
        self.assertEqual(capture.call_args.args[0]['tool_usage']['web_search']['num_requests'], 2)
