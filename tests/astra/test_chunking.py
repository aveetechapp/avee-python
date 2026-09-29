from __future__ import annotations

import json
from typing import Any

import pytest
from aiohttp import web

from avee.astra import (
    FEED_IDS_PER_REQUEST,
    MAX_IDS_PER_URL,
    AstraClient,
    AstraHTTPError,
    AstraValidationError,
    AsyncAstraClient,
)

from .conftest import ACK, FakeAstra, envelope, parsed, ws_update


def ids_of(n: int, offset: int = 0) -> list[str]:
    return [format(i + offset, "064x") for i in range(n)]


async def echo(request: web.Request) -> web.StreamResponse:
    asked = request.query.getall("ids[]")
    return web.json_response(envelope(*(parsed(i, "1", 1) for i in asked)))


def asked(fake: FakeAstra) -> list[list[str]]:
    return [list(r.query.getall("ids[]")) for r in fake.requests]


def test_the_url_limit_is_one_constant() -> None:
    assert MAX_IDS_PER_URL == 200
    assert FEED_IDS_PER_REQUEST == MAX_IDS_PER_URL


@pytest.mark.parametrize(("n", "sizes"), [(1, [1]), (199, [199]), (200, [200]), (201, [200, 1]), (450, [200, 200, 50])])
def test_latest_prices_splits_at_the_url_limit(fake: FakeAstra, n: int, sizes: list[int]) -> None:
    ids = ids_of(n)
    fake.http = echo
    with AstraClient(fake.url) as c:
        got = c.latest_prices(ids, ignore_invalid=True)
    assert [len(chunk) for chunk in asked(fake)] == sizes
    assert all(r.path == "/v2/updates/price/latest" and r.query["ignore_invalid_price_ids"] == "true" for r in fake.requests)
    assert [u.id for u in got] == ids


def test_latest_prices_keeps_request_order_and_collapses_duplicates_across_chunks(fake: FakeAstra) -> None:
    ids = list(reversed(ids_of(250)))
    request = ids[:200] + ["0x" + ids[0].upper(), ids[199]] + ids[200:]
    fake.http = echo
    with AstraClient(fake.url) as c:
        got = c.latest_prices(request)
    assert asked(fake) == [ids[:200], ids[200:]]
    assert [u.id for u in got] == ids


def test_latest_prices_fails_whole_when_a_chunk_fails(fake: FakeAstra) -> None:
    ids = ids_of(201)

    async def handler(request: web.Request) -> web.StreamResponse:
        if len(fake.requests) > 1:
            return web.Response(status=404, text=f"Price ids not found: {ids[200]}")
        return await echo(request)

    fake.http = handler
    with AstraClient(fake.url) as c, pytest.raises(AstraHTTPError) as err:
        c.latest_prices(ids)
    assert err.value.status == 404
    assert len(fake.requests) == 2


def test_latest_prices_keeps_the_server_bound(fake: FakeAstra) -> None:
    fake.http = echo
    with AstraClient(fake.url) as c, pytest.raises(AstraValidationError):
        c.latest_prices(ids_of(501))
    assert fake.requests == []


@pytest.mark.anyio
async def test_async_latest_prices_splits_and_merges(fake: FakeAstra) -> None:
    ids = ids_of(450)
    fake.http = echo
    async with AsyncAstraClient(fake.url) as c:
        got = await c.latest_prices(ids + ids[:10])
    assert [len(chunk) for chunk in asked(fake)] == [200, 200, 50]
    assert [u.id for u in got] == ids


@pytest.mark.anyio
async def test_async_latest_prices_fails_whole_when_a_chunk_fails(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        if len(fake.requests) == 2:
            return web.Response(status=400, text="bad")
        return await echo(request)

    fake.http = handler
    async with AsyncAstraClient(fake.url) as c:
        with pytest.raises(AstraHTTPError):
            await c.latest_prices(ids_of(450))
    assert len(fake.requests) == 2


@pytest.mark.anyio
async def test_sse_refuses_more_ids_than_fit_a_url(fake: FakeAstra) -> None:
    async with AsyncAstraClient(fake.url) as c:
        with pytest.raises(AstraValidationError, match='use transport "ws"'):
            c.subscribe(ids_of(201), transport="sse")
        sub = c.subscribe(ids_of(200) + ids_of(5), transport="sse")
        await sub.aclose()


@pytest.mark.anyio
async def test_ws_takes_more_ids_than_fit_a_url(fake: FakeAstra) -> None:
    ids = ids_of(201)
    received: list[Any] = []

    async def ws_handler(ws: web.WebSocketResponse, _: int) -> None:
        msg = await ws.receive()
        received.append(json.loads(msg.data))
        await ws.send_str(ACK)
        await ws.send_str(ws_update(ids[200], "1", 1))
        await ws.receive()

    fake.ws = ws_handler
    async with AsyncAstraClient(fake.url) as c:
        sub = c.subscribe(ids)
        got = await sub.__anext__()
        await sub.aclose()
    assert got.id == ids[200]
    assert received[0]["ids"] == ids
