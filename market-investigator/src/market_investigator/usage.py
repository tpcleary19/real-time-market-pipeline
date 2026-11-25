"""Provider usage accounting. Unknown usage/cost is never silently made zero."""

from decimal import Decimal
from psycopg.types.json import Jsonb

# Standard synchronous/global USD list prices checked 2026-11-23.
# Keep old snapshots on attempt rows; add a new version when rates change.
PRICING_VERSION = "2026-11-23-standard-v1"
PRICES = {
    "anthropic": {
        "models": ["claude-sonnet-5-5"],
        "input_per_million": "2", "output_per_million": "10",
        "cache_read_per_million": "0.10",
        "cache_write_5m_per_million": "2.50",
        "cache_write_1h_per_million": "4",
        "search_per_request": "0.01",
        "source": "https://platform.claude.com/docs/en/about-claude/pricing",
    },
    "openai": {
        "models": ["gpt-5-mini", "gpt-5-mini-2025-08-07"],
        "input_per_million": "0.25", "output_per_million": "2",
        "cache_read_per_million": "0.025", "search_per_request": "0.01",
        "source": "https://developers.openai.com/api/docs/models/gpt-5-mini",
        "search_source": "https://developers.openai.com/api/docs/pricing",
    },
}


def pricing_snapshot(provider, model):
    rates = PRICES.get(provider)
    if rates is None or model not in rates["models"]:
        return None
    return {"version": PRICING_VERSION, "provider": provider,
            "assumptions": "standard synchronous global routing; no discounts or taxes",
            **rates}


def count(value):
    return value if type(value) is int and value >= 0 else None


def normalize_usage(provider, usage, tool_usage):
    usage = usage if isinstance(usage, dict) else {}
    tool_usage = tool_usage if isinstance(tool_usage, dict) else {}
    metrics = {"input_tokens": count(usage.get("input_tokens")),
               "output_tokens": count(usage.get("output_tokens")),
               "cache_read_tokens": None, "cache_write_tokens": None,
               "reasoning_tokens": None, "billable_searches": None}
    if provider == "anthropic":
        metrics.update(
            cache_read_tokens=count(usage.get("cache_read_input_tokens")),
            cache_write_tokens=count(usage.get("cache_creation_input_tokens")),
            billable_searches=count((usage.get("server_tool_use") or {}).get("web_search_requests")),
        )
    elif provider == "openai":
        details = usage.get("input_tokens_details") or {}
        metrics.update(
            cache_read_tokens=count(details.get("cached_tokens")),
            cache_write_tokens=count(details.get("cache_write_tokens")),
            reasoning_tokens=count((usage.get("output_tokens_details") or {}).get("reasoning_tokens")),
            billable_searches=count((tool_usage.get("web_search") or {}).get("num_requests")),
        )
    return metrics


def estimate_cost(provider, model, usage, metrics, pricing):
    if not pricing or model not in pricing["models"]:
        return None, "unpriced_model"
    if any(metrics[key] is None for key in ("input_tokens", "output_tokens", "cache_read_tokens", "billable_searches")):
        return None, "missing_usage"
    rates = {key: Decimal(value) for key, value in pricing.items()
             if key.endswith("_per_million") or key == "search_per_request"}
    input_tokens = metrics["input_tokens"]
    cached = metrics["cache_read_tokens"]
    cost = Decimal(metrics["output_tokens"]) * rates["output_per_million"]
    cost += Decimal(cached) * rates["cache_read_per_million"]
    if provider == "anthropic":
        writes = metrics["cache_write_tokens"]
        if writes is None:
            return None, "missing_cache_usage"
        creation = usage.get("cache_creation") or {}
        short = count(creation.get("ephemeral_5m_input_tokens"))
        long = count(creation.get("ephemeral_1h_input_tokens"))
        if writes == 0:
            short, long = 0, 0
        if short is None or long is None or short + long != writes:
            return None, "missing_cache_ttl"
        # Claude's input_tokens excludes cache reads and writes.
        cost += Decimal(input_tokens) * rates["input_per_million"]
        cost += Decimal(short) * rates["cache_write_5m_per_million"]
        cost += Decimal(long) * rates["cache_write_1h_per_million"]
    elif provider == "openai":
        if cached > input_tokens:
            return None, "inconsistent_usage"
        # New cache-write pricing is deliberately not guessed for this model.
        if metrics["cache_write_tokens"] not in (0, None):
            return None, "unsupported_cache_write_pricing"
        cost += Decimal(input_tokens - cached) * rates["input_per_million"]
    else:
        return None, "unsupported_provider"
    return cost / Decimal(1_000_000) + Decimal(metrics["billable_searches"]) * rates["search_per_request"], None


def begin_model_request(connection, token):
    # Commit before sending: a crash after this point has potentially billable usage.
    with connection.transaction():
        updated = connection.execute(
            """UPDATE investigation_attempts SET model_request_started_at = clock_timestamp()
               WHERE claim_token = %s AND outcome = 'running'
                 AND model_request_started_at IS NULL""", (token,),
        )
        if updated.rowcount != 1:
            raise RuntimeError("Attempt expired before model request")


def record_response(connection, token, response):
    """Save usage even when the report is incomplete or its claim has expired."""
    with connection.transaction():
        attempt = connection.execute(
            "SELECT provider, pricing_snapshot FROM investigation_attempts WHERE claim_token = %s FOR UPDATE",
            (token,),
        ).fetchone()
        if attempt is None:
            raise RuntimeError("Missing investigation attempt")
        provider = attempt["provider"]
        usage = response.get("usage")
        tools = response.get("tool_usage")
        metrics = normalize_usage(provider, usage, tools)
        price, reason = estimate_cost(provider, response.get("model"), usage or {}, metrics,
                                      attempt["pricing_snapshot"])
        tier = response.get("service_tier")
        if tier not in (None, "standard", "default"):
            price, reason = None, "unsupported_service_tier"
        connection.execute(
            """UPDATE investigation_attempts
               SET response_model = %s, response_service_tier = %s, response_id = %s, response_received_at = clock_timestamp(),
                   model_duration_ms = %s, raw_usage = %s, raw_tool_usage = %s,
                   usage_status = %s, input_tokens = %s, output_tokens = %s,
                   cache_read_tokens = %s, cache_write_tokens = %s, reasoning_tokens = %s,
                   billable_searches = %s, estimated_cost_usd = %s, cost_status = %s,
                   cost_unknown_reason = %s
               WHERE claim_token = %s AND response_received_at IS NULL""",
            (response.get("model"), tier, response.get("response_id"), response.get("duration_ms"),
             Jsonb(usage) if usage is not None else None,
             Jsonb(tools) if tools is not None else None,
             "reported" if usage is not None else "unknown",
             *(metrics[key] for key in ("input_tokens", "output_tokens", "cache_read_tokens",
                                       "cache_write_tokens", "reasoning_tokens", "billable_searches")),
             price, "estimated" if price is not None else "unknown", reason, token),
        )


def finish_attempt(connection, token, outcome, error_class=None):
    """Caller owns the transaction so job and attempt outcomes commit together."""
    connection.execute(
        """UPDATE investigation_attempts
           SET outcome = %s, error_class = %s, finished_at = clock_timestamp(),
               duration_ms = CASE WHEN %s = 'expired' THEN NULL ELSE
                   greatest(0, extract(epoch FROM (clock_timestamp() - started_at)) * 1000) END,
               usage_status = CASE WHEN model_request_started_at IS NULL
                   THEN 'not_requested' ELSE usage_status END,
               estimated_cost_usd = CASE WHEN model_request_started_at IS NULL
                   THEN 0 ELSE estimated_cost_usd END,
               cost_status = CASE WHEN model_request_started_at IS NULL
                   THEN 'not_requested' ELSE cost_status END,
               cost_unknown_reason = CASE WHEN model_request_started_at IS NULL THEN NULL
                   WHEN response_received_at IS NULL THEN 'no_provider_response'
                   ELSE cost_unknown_reason END,
               input_tokens = CASE WHEN model_request_started_at IS NULL THEN 0 ELSE input_tokens END,
               output_tokens = CASE WHEN model_request_started_at IS NULL THEN 0 ELSE output_tokens END,
               cache_read_tokens = CASE WHEN model_request_started_at IS NULL THEN 0 ELSE cache_read_tokens END,
               cache_write_tokens = CASE WHEN model_request_started_at IS NULL THEN 0 ELSE cache_write_tokens END,
               billable_searches = CASE WHEN model_request_started_at IS NULL THEN 0 ELSE billable_searches END
           WHERE claim_token = %s AND outcome = 'running'""",
        (outcome, error_class, outcome, token),
    )
