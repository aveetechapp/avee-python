from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from aiohttp import web

from tests.astra.conftest import ACK, BTC, BTC_ASTRA, FakeAstra, envelope, hold, parsed, sse_event, sse_open, ws_update

UNKNOWN = "1" * 64
PUBLISH_TIME = 1700000000


def feed_metadata() -> dict[str, object]:
    return {
        "id": BTC,
        "attributes": {
            "asset_type": "Crypto",
            "base": "BTC",
            "description": "BITCOIN / US DOLLAR",
            "display_symbol": "BTC/USD",
            "quote_currency": "USD",
            "symbol": "Crypto.BTC/USD",
            "min_channel": "real_time",
            "astra_id": BTC_ASTRA,
            "future": "x",
        },
        "market_hours": {"is_open": True},
    }


def status(stale: bool) -> dict[str, object]:
    feed = {"id": BTC_ASTRA, "symbol": "Crypto.BTC/USD", "status": "trading", "age_seconds": 1.5, "publish_time": 1, "served_publish_time": 1, "sources": 3, "stale": stale}
    return {"ready": True, "feeds": [feed]}


def updates(request: web.Request) -> dict[str, object]:
    return envelope(*(parsed(i.removeprefix("0x"), "6500000000000", PUBLISH_TIME) for i in request.query.getall("ids[]", [])))


async def rest(request: web.Request) -> web.StreamResponse:
    path = request.path
    if path == "/v2/price_feeds":
        return web.json_response([feed_metadata()])
    if path == f"/v2/price_feeds/{BTC}":
        return web.json_response(feed_metadata())
    if path.startswith("/v2/price_feeds/"):
        return web.Response(status=404, text=f"Price ids not found: {path[16:]}")
    if path == "/v2/updates/price/stream":
        resp = await sse_open(request)
        await sse_event(resp, updates(request))
        await hold(request)
        return resp
    if path.count("/") == 5:
        return web.json_response([updates(request)])
    if path.startswith("/v2/updates/price/"):
        return web.json_response(updates(request))
    if path == "/v1/feeds":
        live = {"status": "trading", "price": "6500000", "conf": "10", "expo": -2, "publish_time": 1, "timestamp_ms": 1000, "served_publish_time": 1, "sources": 3}
        return web.json_response([{"id": BTC_ASTRA, "pyth_id": BTC, "symbol": "Crypto.BTC/USD", "category": "crypto", "attributes": {}, "live": live}])
    if path == "/v1/feed-ids":
        entry = {"symbol": "Crypto.BTC/USD", "asset_type": "Crypto", "category": "crypto", "astra_id": BTC_ASTRA, "pyth_id": BTC}
        return web.json_response({"items": [entry], "missing": [UNKNOWN]})
    if path == "/v1/status":
        return web.json_response(status(True), status=503) if "feed" in request.query else web.json_response(status(False))
    if path == "/v1/candles":
        return web.json_response({"s": "ok", "t": [0], "o": [1], "h": [2], "l": [0.5], "c": [1.5], "v": [0]})
    body = {"type": "about:blank", "title": "Not Found", "status": 404, "detail": "no route"}
    return web.json_response(body, status=404, content_type="application/problem+json")


async def ws(socket: web.WebSocketResponse, _: int) -> None:
    async for msg in socket:
        data = json.loads(msg.data)
        if data.get("type") != "subscribe":
            continue
        await socket.send_str(ACK)
        for feed_id in data["ids"]:
            await socket.send_str(ws_update(feed_id, "6500000000000", PUBLISH_TIME))


@pytest.fixture
def astra() -> Iterator[FakeAstra]:
    server = FakeAstra()
    server.http = rest
    server.ws = ws
    server.start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
