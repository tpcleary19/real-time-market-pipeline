from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from market_investigator.investigate import request_report, run_one
from test_intake import EVENT


def response(status="completed", text="No supported explanation found"):
    return SimpleNamespace(status=status, output_text=text, model="test-model", id="resp_test",
                           output=[Mock(model_dump=Mock(return_value={
                               "type": "web_search_call", "status": "completed"}))], usage=None)


class InvestigationTests(unittest.TestCase):
    def setUp(self):
        lookup = patch("market_investigator.investigate.lookup_asset", return_value={
            "outcome": "verified", "asset": {"symbol": "ZZZZ", "exchange": "TEST"}})
        self.lookup = lookup.start()
        self.addCleanup(lookup.stop)

    def test_report_keeps_tool_evidence_and_metadata(self):
        client = Mock()
        client.responses.create.return_value = response()
        result = request_report(client, "test-model", EVENT)
        self.assertEqual(result["response_id"], "resp_test")
        self.assertEqual(result["output"][0]["type"], "web_search_call")
        self.assertIn("No supported explanation", result["report"])

    def test_incomplete_and_empty_responses_cannot_complete_jobs(self):
        client = Mock()
        for result in (response(status="incomplete"), response(text="")):
            client.responses.create.return_value = result
            with self.assertRaises(RuntimeError):
                request_report(client, "test-model", EVENT)

    def test_no_job_does_not_call_model(self):
        client = Mock()
        with patch("market_investigator.investigate.claim_job", return_value=(None, None)):
            self.assertIsNone(run_one(Mock(), client, "test-model"))
        client.responses.create.assert_not_called()

    def test_provider_failure_records_safe_error_and_never_completes(self):
        connection = MagicMock()
        connection.execute.return_value.rowcount = 1
        job = {"event_id": "test", "symbol": "ZZZZ", "spike": EVENT}
        with patch("market_investigator.investigate.claim_job", return_value=(job, "token")):
            with patch("market_investigator.investigate.request_report", side_effect=RuntimeError("secret detail")):
                with self.assertRaises(RuntimeError):
                    run_one(connection, Mock(), "test-model")
        query, params = connection.execute.call_args.args
        self.assertIn("status = 'failed'", query)
        self.assertEqual(params, ("RuntimeError", "test", "token"))

    def test_lost_claim_cannot_overwrite_another_workers_result(self):
        connection = MagicMock()
        connection.execute.return_value.rowcount = 0
        begin = patch("market_investigator.investigate.begin_model_request")
        begin.start()
        self.addCleanup(begin.stop)
        job = {"event_id": "test", "symbol": "ZZZZ", "spike": EVENT}
        with patch("market_investigator.investigate.claim_job", return_value=(job, "token")):
            with patch("market_investigator.investigate.request_report", return_value={"report": "test"}):
                with self.assertRaisesRegex(RuntimeError, "claim expired"):
                    run_one(connection, Mock(), "test-model")
        for call in connection.execute.call_args_list:
            self.assertIn("claim_token = %s", call.args[0])


class ClaudeTests(unittest.TestCase):
    def test_claude_uses_messages_api_and_preserves_citations(self):
        import anthropic
        import httpx

        citation = {"type": "web_search_result_location", "url": "https://example.com/news",
                    "title": "Company announcement", "cited_text": "Reported news",
                    "encrypted_index": "test"}

        def transport(request):
            import json
            body = json.loads(request.content)
            self.assertEqual(request.url.path, "/v1/messages")
            self.assertEqual(body["model"], "claude-test")
            self.assertEqual(body["tools"][0]["max_uses"], 3)
            self.assertEqual(body["tools"][0]["name"], "web_search")
            return httpx.Response(200, json={
                "id": "msg_test", "type": "message", "role": "assistant",
                "model": "claude-test", "stop_reason": "end_turn", "stop_sequence": None,
                "content": [{"type": "text", "text": "Possible context, causation unverified.",
                             "citations": [citation]}],
                "usage": {"input_tokens": 30, "output_tokens": 15},
            })

        with anthropic.Anthropic(api_key="test-not-a-real-key", max_retries=0,
                                 http_client=httpx.Client(transport=httpx.MockTransport(transport))) as client:
            result = request_report(client, "claude-test", EVENT, provider="anthropic")
        self.assertEqual(result["provider"], "anthropic")
        self.assertEqual(result["output"][0]["citations"][0]["url"], citation["url"])
        self.assertEqual(result["usage"]["output_tokens"], 15)

    def test_claude_partial_or_refused_turns_are_not_completed(self):
        client = Mock()
        for stop_reason in ("pause_turn", "max_tokens", "refusal", "tool_use"):
            client.messages.create.return_value = SimpleNamespace(stop_reason=stop_reason)
            with self.assertRaises(RuntimeError):
                request_report(client, "claude-test", EVENT, provider="anthropic")

    def test_web_search_error_inside_successful_response_is_failure(self):
        client = Mock()
        client.messages.create.return_value = SimpleNamespace(
            stop_reason="end_turn", content=[Mock(model_dump=Mock(return_value={
                "type": "web_search_tool_result", "content": {
                    "type": "web_search_tool_result_error", "error_code": "unavailable"}}))])
        with self.assertRaisesRegex(RuntimeError, "web search failed"):
            request_report(client, "claude-test", EVENT, provider="anthropic")

    def test_provider_configuration_selects_claude_key_and_model(self):
        from market_investigator.investigate import provider_settings
        with patch.dict("os.environ", {"INVESTIGATOR_PROVIDER": "anthropic",
                                       "ANTHROPIC_MODEL": "claude-test", "ANTHROPIC_API_KEY": "test-key"}, clear=True):
            self.assertEqual(provider_settings(), ("anthropic", "ANTHROPIC_API_KEY", "claude-test"))
        with patch.dict("os.environ", {"INVESTIGATOR_PROVIDER": "invalid"}, clear=True):
            with self.assertRaises(ValueError):
                provider_settings()

    def test_claudekey_alias_selects_claude_without_explicit_provider(self):
        from market_investigator.investigate import provider_settings
        with patch.dict("os.environ", {"CLAUDEKEY": "test-key"}, clear=True):
            self.assertEqual(provider_settings()[:2], ("anthropic", "CLAUDEKEY"))
        with patch.dict("os.environ", {"CLAUDEKEY": "test-key", "ANTHROPIC_API_KEY": "standard"}, clear=True):
            self.assertEqual(provider_settings()[:2], ("anthropic", "ANTHROPIC_API_KEY"))
        with patch.dict("os.environ", {"CLAUDEKEY": "test-key", "INVESTIGATOR_PROVIDER": "openai"}, clear=True):
            self.assertEqual(provider_settings()[:2], ("openai", "OPENAI_API_KEY"))


class AssetGateTests(unittest.TestCase):
    def test_unknown_asset_finishes_without_paid_research(self):
        connection, client = MagicMock(), Mock()
        connection.execute.return_value.rowcount = 1
        job = {"event_id": "test", "symbol": "ZZZZ", "spike": EVENT}
        with patch("market_investigator.investigate.claim_job", return_value=(job, "token")):
            with patch("market_investigator.investigate.lookup_asset", return_value={"outcome": "not_found"}):
                result = run_one(connection, client, "test-model")
        self.assertEqual(result["outcome"], "not_found")
        self.assertIsNone(result["usage"])
        client.responses.create.assert_not_called()
        client.messages.create.assert_not_called()

    def test_alpaca_outage_fails_without_model_call(self):
        connection, client = MagicMock(), Mock()
        job = {"event_id": "test", "symbol": "ZZZZ", "spike": EVENT}
        with patch("market_investigator.investigate.claim_job", return_value=(job, "token")):
            with patch("market_investigator.investigate.lookup_asset", side_effect=TimeoutError):
                with self.assertRaises(TimeoutError):
                    run_one(connection, client, "test-model")
        client.responses.create.assert_not_called()
        self.assertIn("status = 'failed'", connection.execute.call_args.args[0])
