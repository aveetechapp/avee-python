from __future__ import annotations

import json
from collections.abc import Awaitable, Callable

import pytest
from aiohttp import web

from avee.astra import AstraClient, AstraHTTPError, AstraValidationError, AsyncAstraClient

from .conftest import BTC, BTC_ASTRA, ETH, FakeAstra


def report(stale: bool) -> dict[str, object]:
    entry = {"id": BTC_ASTRA, "symbol": "Crypto.BTC/USD", "status": "trading", "age_seconds": 1.5,
             "publish_time": 1, "served_publish_time": 1, "sources": 3, "stale": stale}
    return {"ready": True, "feeds": [entry]}


def reply(body: object, status: int) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.json_response(body, status=status)

    return handler


def test_sends_the_normalised_id(fake: FakeAstra) -> None:
    fake.http = reply(report(False), 200)
    with AstraClient(fake.url) as c:
        got = c.status(feed="0x" + BTC.upper())
    assert not got.feeds[0].stale
    assert fake.requests[0].query["feed"] == BTC


def test_returns_the_report_on_503(fake: FakeAstra) -> None:
    fake.http = reply(report(True), 503)
    with AstraClient(fake.url, max_retries=2, max_retry_delay=0.02) as c:
        got = c.status(feed=BTC)
    assert got.feeds[0].stale and len(fake.requests) == 1


def test_other_answers_still_fail(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        if request.query["feed"] == BTC:
            return web.Response(status=503, text="upstream down")
        body = {"type": "about:blank", "title": "Not Found", "status": 404, "detail": "unknown feed"}
        return web.Response(status=404, text=json.dumps(body), content_type="application/problem+json")

    fake.http = handler
    with AstraClient(fake.url, max_retries=1, max_retry_delay=0.02) as c:
        with pytest.raises(AstraHTTPError):
            c.status(feed=BTC)
        assert len(fake.requests) == 2
        with pytest.raises(AstraHTTPError, match="unknown feed"):
            c.status(feed=ETH)
        with pytest.raises(AstraValidationError):
            c.status(feed="nope")


def test_without_feed_a_503_is_an_error(fake: FakeAstra) -> None:
    fake.http = reply(report(True), 503)
    with AstraClient(fake.url, max_retries=0) as c, pytest.raises(AstraHTTPError):
        c.status()


@pytest.mark.anyio
async def test_async_status_of_one_feed(fake: FakeAstra) -> None:
    fake.http = reply(report(True), 503)
    async with AsyncAstraClient(fake.url) as c:
        got = await c.status(feed=BTC)
    assert got.feeds[0].stale


def problem(status: int, detail: str) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    async def handler(_: web.Request) -> web.StreamResponse:
        body = {"type": "about:blank", "title": "x", "status": status, "detail": detail}
        return web.Response(status=status, body=json.dumps(body), content_type="application/problem+json")

    return handler


def test_a_503_problem_is_an_error_not_a_report(fake: FakeAstra) -> None:
    fake.http = problem(503, "draining")
    with AstraClient(fake.url, max_retries=1, max_retry_delay=0.02) as c, pytest.raises(AstraHTTPError) as err:
        c.status(feed=BTC)
    assert err.value.status == 503 and len(fake.requests) == 2


def test_the_server_400_is_not_retried(fake: FakeAstra) -> None:
    fake.http = problem(400, "malformed feed")
    with AstraClient(fake.url, max_retries=2, max_retry_delay=0.02) as c, pytest.raises(AstraHTTPError, match="malformed feed"):
        c.status(feed=BTC)
    assert len(fake.requests) == 1


def test_an_empty_feed_reads_the_whole_report(fake: FakeAstra) -> None:
    fake.http = reply(report(False), 200)
    with AstraClient(fake.url) as c:
        c.status(feed="")
    assert dict(fake.requests[0].query) == {}
