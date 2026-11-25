import unittest
from unittest.mock import patch

import httpx
from pydantic import ValidationError

from market_investigator.assets import lookup_asset

ASSET = {"id": "asset-id", "symbol": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ",
         "class": "us_equity", "status": "active", "tradable": False}


class AssetTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict("os.environ", {"APCA_API_KEY_ID": "test-key",
                                       "APCA_API_SECRET_KEY": "test-secret"}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def lookup(self, status, payload, symbol="AAPL"):
        with httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(status, json=payload))) as client:
            return lookup_asset(symbol, client)

    def test_data_only_asset_is_valid_and_keeps_identity(self):
        result = self.lookup(200, ASSET)
        self.assertEqual(result["outcome"], "verified")
        self.assertEqual(result["asset"]["exchange"], "NASDAQ")
        self.assertFalse(result["asset"]["tradable"])
        self.assertIn("not historical", result["scope"])

    def test_404_is_not_found_but_auth_rate_limits_and_outages_raise(self):
        self.assertEqual(self.lookup(404, {})["outcome"], "not_found")
        for status in (401, 403, 429, 500):
            with self.assertRaises(httpx.HTTPStatusError):
                self.lookup(status, {})

    def test_malformed_and_mismatched_assets_are_not_verified(self):
        with self.assertRaises(ValidationError):
            self.lookup(200, {"symbol": "AAPL"})
        with self.assertRaises(ValueError):
            self.lookup(200, ASSET | {"symbol": "OTHER"})

    def test_crypto_is_not_passed_as_equity(self):
        self.assertEqual(self.lookup(200, ASSET | {"class": "crypto"})["outcome"],
                         "unsupported_asset_class")

    def test_symbol_is_encoded_and_uses_alpaca_headers(self):
        def respond(request):
            self.assertEqual(request.url.raw_path, b"/v2/assets/BRK%2FB")
            self.assertEqual(request.headers["APCA-API-KEY-ID"], "test-key")
            return httpx.Response(404)
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            lookup_asset("BRK/B", client)
