from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest
from aiohttp import web

from avee.astra import (
    AstraClient,
    AstraConnectionError,
    AstraHTTPError,
    AstraTimeoutError,
    AstraValidationError,
    AsyncAstraClient,
)

from .conftest import BTC, BTC_ASTRA, ETH, FakeAstra, envelope, parsed

FEED_META = {
    "id": BTC,
    "attributes": {
        "asset_type": "Crypto", "description": "d", "display_symbol": "BTC/USD", "quote_currency": "USD",
        "symbol": "Crypto.BTC/USD", "min_channel": "real_time", "astra_id": BTC_ASTRA,
    },
    "market_hours": {"is_open": True, "next_open": None, "next_close": None},
}


def respond(body: object, status: int = 200, **headers: str) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    async def handler(_: web.Request) -> web.StreamResponse:
        if isinstance(body, str):
            return web.Response(status=status, text=body, headers=headers)
        return web.json_response(body, status=status, headers=headers)

    return handler


def client(fake: FakeAstra, **kwargs: object) -> AstraClient:
    return AstraClient(fake.url, **{"max_retry_delay": 2.0, **kwargs})  # type: ignore[arg-type]


def test_price_feeds_sends_filters(fake: FakeAstra) -> None:
    fake.http = respond([FEED_META])
    with client(fake) as c:
        feeds = c.price_feeds(query="btc", asset_type="crypto")
    assert [f.symbol for f in feeds] == ["Crypto.BTC/USD"]
    req = fake.requests[0]
    assert req.path == "/v2/price_feeds" and req.query["query"] == "btc" and req.query["asset_type"] == "crypto"


def test_base_path_prefix_and_id_normalization(fake: FakeAstra) -> None:
    fake.http = respond(FEED_META)
    with AstraClient(fake.url + "/hermes") as c:
        c.price_feed("0x" + BTC.upper())
    assert fake.requests[0].path == f"/hermes/v2/price_feeds/{BTC}"


def test_latest_prices_in_one_request(fake: FakeAstra) -> None:
    fake.http = respond(envelope(parsed(BTC, "1", 5), parsed(ETH, "2", 5)))
    with client(fake) as c:
        prices = c.latest_prices([BTC, "0x" + ETH, BTC], ignore_invalid=True)
    assert [p.id for p in prices] == [BTC, ETH]
    assert len(fake.requests) == 1
    q = fake.requests[0].query
    assert q.getall("ids[]") == [BTC, ETH] and q["ignore_invalid_price_ids"] == "true"


def test_inputs_are_validated_before_any_request(fake: FakeAstra) -> None:
    fake.http = respond(envelope())
    many = [f"{i:064x}" for i in range(101)]
    with client(fake) as c:
        for call in (
            lambda: c.latest_prices(["nope"]),
            lambda: c.prices_at(1, many),
            lambda: c.prices_at(-1, [BTC]),
            lambda: c.prices_in_interval(1, 61, [BTC]),
            lambda: c.candles("x", "60", 10, 5),
            lambda: c.candles("", "60", 0, 1),
            lambda: c.candles("x", "6 0", 0, 1),
            lambda: c.candles("x", "\u0661", 0, 1),
            lambda: c.candles("x", "123456789", 0, 1),
        ):
            with pytest.raises(AstraValidationError):
                call()
    assert fake.requests == []


def test_historical_routes(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        if request.path == "/v2/updates/price/100":
            return web.json_response(envelope(parsed(BTC, "1", 100)))
        return web.json_response([envelope(parsed(BTC, "1", 100)), envelope(parsed(BTC, "2", 101))])

    fake.http = handler
    with client(fake) as c:
        assert c.prices_at(100, [BTC])[0].price.publish_time == 100
        series = c.prices_in_interval(100, 1, [BTC], unique=False)
    assert [u.price.price for u in series] == ["1", "2"]
    assert fake.requests[1].path == "/v2/updates/price/100/1" and fake.requests[1].query["unique"] == "false"


def test_native_routes(fake: FakeAstra) -> None:
    live = {"status": "trading", "price": "1", "conf": "0", "expo": -2, "publish_time": 1, "timestamp_ms": 1000,
            "served_publish_time": 1, "sources": 3}

    async def handler(request: web.Request) -> web.StreamResponse:
        if request.path == "/v1/feeds":
            return web.json_response([{"id": BTC_ASTRA, "pyth_id": BTC, "symbol": "S", "category": "crypto", "attributes": {}, "live": live}])
        if request.path == "/v1/status":
            return web.json_response({"ready": True, "feeds": []})
        return web.json_response({"s": "ok", "t": [0], "o": [1], "h": [2], "l": [0.5], "c": [1.5], "v": [0]})

    fake.http = handler
    with client(fake) as c:
        feed = c.feeds(category="crypto")[0]
        assert feed.live.price is not None and feed.live.price.to_float() == 0.01
        assert c.status().ready
        assert len(c.candles("Crypto.BTC/USD", "60", 0, 60)) == 1
    q = fake.requests[2].query
    assert (q["feed"], q["resolution"], q["from"], q["to"]) == ("Crypto.BTC/USD", "60", "0", "60")


def test_hermes_text_error_is_not_retried(fake: FakeAstra) -> None:
    fake.http = respond(f"Price ids not found: {BTC}", 404)
    with client(fake) as c, pytest.raises(AstraHTTPError) as info:
        c.latest_prices([BTC])
    assert info.value.status == 404 and "Price ids not found" in info.value.body and not info.value.retryable
    assert len(fake.requests) == 1


def test_udf_error_message(fake: FakeAstra) -> None:
    fake.http = respond({"s": "error", "errmsg": "unknown feed"}, 404)
    with client(fake) as c, pytest.raises(AstraHTTPError, match="unknown feed"):
        c.candles("nope", "60", 0, 1)


def test_problem_is_retried_then_raised(fake: FakeAstra) -> None:
    problem = {"type": "about:blank", "title": "History unavailable", "status": 503, "detail": "charts database down"}

    async def handler(_: web.Request) -> web.StreamResponse:
        return web.json_response(problem, status=503, content_type="application/problem+json")

    fake.http = handler
    with client(fake, max_retries=2, max_retry_delay=0.05) as c, pytest.raises(AstraHTTPError) as info:
        c.prices_at(1, [BTC])
    assert info.value.problem is not None and info.value.problem.detail == "charts database down"
    assert len(fake.requests) == 3


def test_retry_after_is_honoured(fake: FakeAstra) -> None:
    calls = 0

    async def handler(_: web.Request) -> web.StreamResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=429, text="slow down", headers={"Retry-After": "1"})
        return web.json_response(envelope(parsed(BTC, "1", 1)))

    fake.http = handler
    started = time.monotonic()
    with client(fake) as c:
        c.latest_prices([BTC])
    assert time.monotonic() - started >= 0.95 and calls == 2


def test_retry_after_beyond_cap_is_not_awaited(fake: FakeAstra) -> None:
    fake.http = respond("later", 429, **{"Retry-After": "120"})
    with client(fake, max_retry_delay=1.0) as c, pytest.raises(AstraHTTPError) as info:
        c.latest_prices([BTC])
    assert info.value.retry_after == 120 and len(fake.requests) == 1


def test_slow_request_times_out_and_is_retried(fake: FakeAstra) -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        await asyncio.sleep(1)
        return web.json_response(envelope())

    fake.http = handler
    with client(fake, timeout=0.1, max_retries=1, max_retry_delay=0.02) as c, pytest.raises(AstraTimeoutError):
        c.status()
    assert len(fake.requests) == 2


def test_refused_connection() -> None:
    with AstraClient("http://127.0.0.1:1", max_retries=0) as c, pytest.raises(AstraConnectionError):
        c.status()


def test_response_size_is_bounded(fake: FakeAstra) -> None:
    fake.http = respond([FEED_META, FEED_META, FEED_META])
    with client(fake, max_response_bytes=256) as c, pytest.raises(AstraValidationError, match="exceeds 256 bytes"):
        c.price_feeds()


def test_malformed_bodies(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        if request.path == "/v1/status":
            return web.Response(text="<html>")
        return web.json_response({"parsed": [{"id": BTC, "price": {"price": "1.5", "conf": "0", "expo": -8, "publish_time": 1}}]})

    fake.http = handler
    with client(fake) as c:
        with pytest.raises(AstraValidationError, match="not JSON"):
            c.status()
        with pytest.raises(AstraValidationError):
            c.latest_prices([BTC])


def test_options_are_validated() -> None:
    for kwargs in (
        {"base_url": "ftp://x"},
        {"base_url": "not a url"},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"timeout": float("inf")},
        {"max_retry_delay": float("nan")},
        {"max_retries": -1},
        {"max_retries": True},
        {"max_retries": float("nan")},
        {"max_response_bytes": 0},
        {"max_response_bytes": 0.5},
        {"max_response_bytes": float("inf")},
    ):
        with pytest.raises(AstraValidationError):
            AstraClient(**kwargs)  # type: ignore[arg-type]
    with AstraClient(max_retries=1.0, max_response_bytes=1e6) as legacy:  # type: ignore[arg-type]
        assert legacy._config.max_retries == 1 and legacy._config.max_response_bytes == 1_000_000


@pytest.mark.anyio
async def test_async_client_mirrors_the_sync_one(fake: FakeAstra) -> None:
    fake.http = respond(envelope(parsed(BTC, "1", 5)))
    async with AsyncAstraClient(fake.url) as c:
        prices = await c.latest_prices([BTC])
    assert prices[0].price.price == "1"


@pytest.mark.anyio
async def test_async_client_retries_and_raises(fake: FakeAstra) -> None:
    fake.http = respond("busy", 503)
    async with AsyncAstraClient(fake.url, max_retries=1, max_retry_delay=0.02) as c:
        with pytest.raises(AstraHTTPError):
            await c.status()
    assert len(fake.requests) == 2


def test_deeply_nested_json_is_a_validation_error(fake: FakeAstra) -> None:
    fake.http = respond("[" * 100_000 + "]" * 100_000, 200)
    with client(fake, max_response_bytes=1 << 20) as c, pytest.raises(AstraValidationError, match="not JSON"):
        c.status()


def test_retry_after_date_without_zone_is_utc() -> None:
    from avee.astra._transport import parse_retry_after

    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 -0000") == 0.0
    assert parse_retry_after("soon") is None
