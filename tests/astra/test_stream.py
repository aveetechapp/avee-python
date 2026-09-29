from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest
from aiohttp import WSMsgType, web

from avee.astra import (
    AstraConnectionError,
    AstraError,
    AstraHTTPError,
    AstraSubscriptionError,
    AstraTimeoutError,
    AstraValidationError,
    AsyncAstraClient,
    Price,
    PriceUpdate,
    Subscription,
)
from avee.astra.stream import SseParser

from .conftest import ACK, BTC, ETH, FakeAstra, envelope, hold, parsed, sse_event, sse_open, ws_update

pytestmark = pytest.mark.anyio

FAST: dict[str, Any] = {"reconnect_base_delay": 0.01, "reconnect_max_delay": 0.03}


async def take(sub: Subscription, n: int, timeout: float = 10) -> list[PriceUpdate]:
    out: list[PriceUpdate] = []

    async def collect() -> None:
        async for u in sub:
            out.append(u)
            if len(out) == n:
                return

    await asyncio.wait_for(collect(), timeout)
    return out


async def until(cond: Any, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


def test_sse_parser_framing_and_limits() -> None:
    got: list[bytes] = []
    p = SseParser(1024, got.append)
    p.feed(b": keepalive\r\n\r\nda")
    p.feed(b'ta: {"a":1}\r')
    p.feed(b"\n\ndata: x\ndata:y\nevent: e\nid: 1\n\n")
    assert got == [b'{"a":1}', b"x\ny"]
    with pytest.raises(AstraError, match="line exceeds"):
        SseParser(8, got.append).feed(b"data: 123456789")
    with pytest.raises(AstraError, match="event exceeds"):
        SseParser(8, got.append).feed(b"data: 12345\ndata: 12345\n")


async def test_sse_reconnects_and_drops_the_replayed_value(fake: FakeAstra) -> None:
    release = asyncio.Event()
    loop = asyncio.get_running_loop()
    connects = 0

    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal connects
        connects += 1
        resp = await sse_open(request)
        if connects == 1:
            await sse_event(resp, envelope(parsed(BTC, "1", 10)))
            return resp
        while not release.is_set():
            await asyncio.sleep(0.01)
        await sse_event(resp, envelope(parsed(BTC, "1", 10)))
        await sse_event(resp, envelope(parsed(BTC, "2", 11)))
        await hold(request)
        return resp

    fake.http = handler
    states: list[str] = []
    async with AsyncAstraClient(fake.url) as c:
        async with c.subscribe([BTC], transport="sse", channel="real_time", on_state_change=states.append, **FAST) as sub:
            assert [u.price.price for u in await take(sub, 1)] == ["1"]
            loop.call_soon(release.set)
            assert [u.price.price for u in await take(sub, 1)] == ["2"]
            assert (sub.stats.duplicates, sub.stats.reconnects, sub.stats.connects) == (1, 1, 2)
    assert states[:3] == ["open", "reconnecting", "open"] and states[-1] == "closed"
    q = fake.requests[0].query
    assert q.getall("ids[]") == [BTC] and q["channel"] == "real_time"


async def test_sse_not_found_is_fatal(fake: FakeAstra) -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.Response(status=404, text=f"Price ids not found: {BTC}")

    fake.http = handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c:
        sub = c.subscribe([BTC], transport="sse", on_error=errors.append, **FAST)
        with pytest.raises(AstraHTTPError):
            await take(sub, 1)
        assert isinstance(sub.error, AstraHTTPError) and len(errors) == 1 and len(fake.requests) == 1
        await sub.aclose()


async def test_sse_retry_after_delays_the_reconnect(fake: FakeAstra) -> None:
    calls = 0

    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=429, text="too many", headers={"Retry-After": "1"})
        resp = await sse_open(request)
        await sse_event(resp, envelope(parsed(BTC, "1", 1)))
        await hold(request)
        return resp

    fake.http = handler
    started = time.monotonic()
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], transport="sse", **FAST) as sub:
        await take(sub, 1)
    assert time.monotonic() - started >= 0.95


async def test_sse_idle_stream_is_reconnected(fake: FakeAstra) -> None:
    calls = 0

    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal calls
        calls += 1
        resp = await sse_open(request)
        if calls > 1:
            await sse_event(resp, envelope(parsed(BTC, "1", 1)))
        await hold(request)
        return resp

    fake.http = handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c:
        async with c.subscribe([BTC], transport="sse", idle_timeout=0.2, on_error=errors.append, **FAST) as sub:
            await take(sub, 1)
    assert isinstance(errors[0], AstraTimeoutError)


async def test_sse_malformed_event_is_dropped(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        resp = await sse_open(request)
        await resp.write(b"data: {not json\n\n")
        bad = parsed(BTC, "1", 1)
        bad["price"] = {"price": "1.5", "conf": "0", "expo": -8, "publish_time": 1}
        await sse_event(resp, envelope(bad))
        await sse_event(resp, envelope(parsed(BTC, "2", 2)))
        await hold(request)
        return resp

    fake.http = handler
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], transport="sse", benchmarks_only=True) as sub:
        assert (await take(sub, 1))[0].price.price == "2"
        assert sub.stats.invalid == 2
    assert fake.requests[0].query["benchmarks_only"] == "true"


async def test_subscribe_validates_options() -> None:
    async with AsyncAstraClient("http://127.0.0.1:1") as c:
        for kwargs in (
            {"benchmarks_only": True},
            {"transport": "pigeon"},
            {"idle_timeout": 0},
            {"idle_timeout": float("nan")},
            {"reconnect_max_delay": float("inf")},
            {"stable_after": -1},
            {"max_message_bytes": 0},
        ):
            with pytest.raises(AstraValidationError):
                c.subscribe([BTC], **kwargs)
        with pytest.raises(AstraValidationError):
            c.subscribe(["nope"])


async def test_ws_subscribe_stream_and_clean_close(fake: FakeAstra) -> None:
    received: list[Any] = []
    close_codes: list[int | None] = []

    async def ws_handler(ws: web.WebSocketResponse, _: int) -> None:
        msg = await ws.receive()
        received.append(json.loads(msg.data))
        await ws.send_str(ACK)
        await ws.send_str(ws_update(BTC, "1", 1))
        await ws.send_str(ws_update(ETH, "2", 1))
        msg = await ws.receive()
        close_codes.append(ws.close_code if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING) else None)

    fake.ws = ws_handler
    before = set(asyncio.all_tasks())
    async with AsyncAstraClient(fake.url) as c:
        sub = c.subscribe(["0x" + BTC, ETH], channel="fixed_rate@200ms")
        got = await take(sub, 2)
        await sub.aclose()
    assert [u.id for u in got] == [BTC, ETH]
    assert got[0].metadata is not None and got[0].metadata.receive_time == 1
    assert received[0] == {"type": "subscribe", "ids": [BTC, ETH], "verbose": True, "binary": False, "ignore_invalid_price_ids": False}
    assert fake.requests[0].query["channel"] == "fixed_rate@200ms"
    assert sub.state == "closed" and sub.error is None
    await until(lambda: close_codes == [1000])
    assert set(asyncio.all_tasks()) <= before


async def test_ws_refused_subscription_is_fatal(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, _: int) -> None:
        await ws.receive()
        await ws.send_str(json.dumps({"type": "response", "status": "error", "error": f"Price feed(s) with id(s) {BTC} not found"}))
        await ws.receive()

    fake.ws = ws_handler
    async with AsyncAstraClient(fake.url) as c:
        sub = c.subscribe([BTC], **FAST)
        with pytest.raises(AstraSubscriptionError):
            await take(sub, 1)
        await sub.aclose()
    assert fake.ws_connections == 1


async def test_ws_resubscribes_after_server_close(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, n: int) -> None:
        await ws.receive()
        await ws.send_str(ACK)
        await ws.send_str(ws_update(BTC, "1", 1))
        if n == 1:
            await ws.send_str(json.dumps({"type": "response", "status": "error", "error": "Connection timeout reached, reconnect"}))
            await ws.close(code=1000)
            return
        await ws.send_str(ws_update(BTC, "2", 2))
        await ws.receive()

    fake.ws = ws_handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], on_error=errors.append, **FAST) as sub:
        got = await take(sub, 2)
        assert sub.stats.duplicates == 1
    assert [u.price.price for u in got] == ["1", "2"] and fake.ws_connections == 2
    assert "Astra: Connection timeout reached, reconnect" in [str(e) for e in errors]


async def test_ws_abnormal_close_reconnects(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, n: int) -> None:
        await ws.receive()
        await ws.send_str(ACK)
        if n == 1:
            await ws.close(code=1008, message=b"slow consumer")
            return
        await ws.send_str(ws_update(BTC, "1", 1))
        await ws.receive()

    fake.ws = ws_handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], on_error=errors.append, **FAST) as sub:
        await take(sub, 1)
    assert "1008: slow consumer" in str(errors[0])


async def test_ws_unanswered_ping_reconnects(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, n: int) -> None:
        await ws.receive()
        await ws.send_str(ACK)
        if n == 1:
            await asyncio.sleep(3)
            return
        await ws.send_str(ws_update(BTC, "1", 1))
        await ws.receive()

    fake.ws = ws_handler
    errors: list[AstraError] = []
    fake_ws_no_pong(fake)
    async with AsyncAstraClient(fake.url) as c:
        async with c.subscribe([BTC], idle_timeout=0.4, on_error=errors.append, **FAST) as sub:
            await take(sub, 1)
    assert isinstance(errors[0], AstraTimeoutError)


def fake_ws_no_pong(fake: FakeAstra) -> None:
    original = fake._dispatch

    async def dispatch(request: web.Request) -> web.StreamResponse:
        if request.path == "/ws" and fake.ws is not None and fake.ws_connections == 0:
            fake.requests.append(request)
            ws = web.WebSocketResponse(autoping=False)
            await ws.prepare(request)
            fake.ws_connections += 1
            await fake.ws(ws, fake.ws_connections)
            return ws
        return await original(request)

    fake._dispatch = dispatch  # type: ignore[method-assign]


async def test_ws_drops_malformed_and_unrequested_messages(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, _: int) -> None:
        await ws.receive()
        for m in (
            ACK,
            "{broken",
            json.dumps({"type": "price_update", "price_feed": {"id": BTC, "price": {"price": "abc"}}}),
            json.dumps({"type": "future_event", "payload": 1}),
            ws_update(ETH, "9", 1),
            ws_update(BTC, "1", 1),
        ):
            await ws.send_str(m)
        await ws.receive()

    fake.ws = ws_handler
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC]) as sub:
        assert (await take(sub, 1))[0].id == BTC
        assert sub.stats.invalid == 2


async def test_ws_oversized_message_ends_the_connection(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, n: int) -> None:
        await ws.receive()
        await ws.send_str(ACK)
        await ws.send_str("x" * 4096 if n == 1 else ws_update(BTC, "1", 1))
        await ws.receive()

    fake.ws = ws_handler
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], max_message_bytes=1024, **FAST) as sub:
        await take(sub, 1)
    assert fake.ws_connections == 2


async def test_slow_consumer_gets_the_latest_price_per_feed(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, _: int) -> None:
        await ws.receive()
        await ws.send_str(ACK)
        for i in range(1, 51):
            await ws.send_str(ws_update(BTC, str(i), i))
        await ws.send_str(ws_update(ETH, "7", 1))
        await ws.receive()

    fake.ws = ws_handler
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC, ETH]) as sub:
        await until(lambda: sub.stats.coalesced == 49)
        await asyncio.sleep(0.05)
        got = await take(sub, 2)
    assert [(u.id, u.price.price) for u in got] == [(BTC, "50"), (ETH, "7")]


async def test_many_subscriptions_leave_no_tasks(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        resp = await sse_open(request)
        await sse_event(resp, envelope(parsed(BTC, "1", 1)))
        await hold(request)
        return resp

    fake.http = handler
    before = set(asyncio.all_tasks())
    async with AsyncAstraClient(fake.url) as c:
        for _ in range(20):
            async with c.subscribe([BTC], transport="sse") as sub:
                await take(sub, 1)
    assert set(asyncio.all_tasks()) <= before


class FlakyTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, sink: Any) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("boom")
        sink.opened()
        sink.update(PriceUpdate(BTC, Price("1", "0", -8, 1), Price("1", "0", -8, 1)))
        await asyncio.Event().wait()


async def test_unexpected_transport_exception_is_reported_and_retried() -> None:
    errors: list[AstraError] = []
    transport = FlakyTransport()
    before = set(asyncio.all_tasks())
    async with Subscription(
        [BTC], transport, base_delay=0.01, max_delay=0.02, stable_after=60, on_error=errors.append, on_state_change=None
    ) as sub:
        got = await take(sub, 1)
    assert got[0].price.price == "1" and transport.calls == 2
    assert isinstance(errors[0], AstraConnectionError) and isinstance(errors[0].__cause__, RuntimeError)
    assert sub.state == "closed" and set(asyncio.all_tasks()) <= before


async def test_sse_deeply_nested_event_is_dropped(fake: FakeAstra) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        resp = await sse_open(request)
        await resp.write(b"data: " + b"[" * 100_000 + b"]" * 100_000 + b"\n\n")
        await sse_event(resp, envelope(parsed(BTC, "2", 2)))
        await hold(request)
        return resp

    fake.http = handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c, c.subscribe([BTC], transport="sse", on_error=errors.append) as sub:
        assert (await take(sub, 1))[0].price.price == "2"
        assert sub.stats.invalid == 1 and sub.stats.reconnects == 0
    assert isinstance(errors[0], AstraValidationError)


async def test_ws_handshake_problem_is_retried_and_not_found_is_fatal(fake: FakeAstra) -> None:
    problem = {"type": "about:blank", "title": "Service Unavailable", "status": 503, "detail": "warming up"}
    calls = 0

    async def handler(_: web.Request) -> web.StreamResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.json_response(problem, status=503, content_type="application/problem+json", headers={"Retry-After": "0"})
        return web.Response(status=404, text="gone")

    fake.http = handler
    errors: list[AstraError] = []
    before = set(asyncio.all_tasks())
    async with AsyncAstraClient(fake.url) as c:
        sub = c.subscribe([BTC], on_error=errors.append, **FAST)
        with pytest.raises(AstraHTTPError) as info:
            await take(sub, 1)
        await sub.aclose()
    assert info.value.status == 404 and sub.error is info.value
    first = errors[0]
    assert isinstance(first, AstraHTTPError) and first.status == 503 and first.retry_after == 0.0
    assert first.problem is not None and first.problem.detail == "warming up" and "warming up" in str(first)
    assert calls == 2 and set(asyncio.all_tasks()) <= before


async def test_ws_missing_ack_times_out_and_reconnects(fake: FakeAstra) -> None:
    async def ws_handler(ws: web.WebSocketResponse, n: int) -> None:
        await ws.receive()
        if n == 1:
            await ws.receive()
            return
        await ws.send_str(ACK)
        await ws.send_str(ws_update(BTC, "1", 1))
        await ws.receive()

    fake.ws = ws_handler
    errors: list[AstraError] = []
    async with AsyncAstraClient(fake.url) as c:
        async with c.subscribe([BTC], idle_timeout=0.3, on_error=errors.append, **FAST) as sub:
            await take(sub, 1)
    assert isinstance(errors[0], AstraTimeoutError) and "subscription response" in str(errors[0])
    assert fake.ws_connections == 2
