"""Read-only asset identity lookup using the existing Alpaca credentials."""

from datetime import datetime, timezone
import os
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, StrictBool


class Asset(BaseModel):
    id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    name: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    asset_class: str = Field(alias="class", min_length=1)
    status: str = Field(min_length=1)
    tradable: StrictBool


def lookup_asset(symbol, client=None):
    key = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise ValueError("Set APCA_API_KEY_ID and APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
    if base_url not in ("https://paper-api.alpaca.markets", "https://api.alpaca.markets"):
        raise ValueError("APCA_API_BASE_URL must be Alpaca's paper or live Trading API host")
    url = f"{base_url}/v2/assets/{quote(symbol, safe='')}"
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    if client is None:
        with httpx.Client(timeout=15, follow_redirects=False) as owned_client:
            return _lookup(owned_client, url, headers, symbol)
    return _lookup(client, url, headers, symbol)


def _lookup(client, url, headers, symbol):
    response = client.get(url, headers=headers)
    metadata = {"source": "alpaca_assets", "url": url,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "scope": "current asset catalog; not historical identity or price verification"}
    if response.status_code == 404:
        return metadata | {"outcome": "not_found", "symbol": symbol}
    response.raise_for_status()
    asset = Asset.model_validate(response.json())
    if asset.symbol != symbol:
        raise ValueError("Alpaca returned a different symbol")
    outcome = "verified" if asset.asset_class == "us_equity" else "unsupported_asset_class"
    return metadata | {"outcome": outcome, "asset": asset.model_dump(by_alias=True)}
