from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import pytest
from aiohttp import web

BTC = "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
ETH = "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"
BTC_ASTRA = "1de769477ecf66f69ca287676658956a211c207a9ca608ad5235bd095b02f4b2"
ACK = json.dumps({"type": "response", "status": "success"})

HttpHandler = Callable[[web.Request], Awaitable[web.StreamResponse]]
WsHandler = Callable[[web.WebSocketResponse, int], Awaitable[None]]


def price(value: str, publish_time: int, expo: int = -8) -> dict[str, Any]:
    return {"price": value, "conf": "1000", "expo": expo, "publish_time": publish_time}


def parsed(feed_id: str, value: str, publish_time: int) -> dict[str, Any]:
    return {
        "id": feed_id,
        "price": price(value, publish_time),
        "ema_price": price(value, publish_time),
        "metadata": {"slot": 0, "proof_available_time": publish_time, "prev_publish_time": publish_time - 1},
    }


def envelope(*items: dict[str, Any]) -> dict[str, Any]:
    return {"binary": {"encoding": "hex", "data": []}, "parsed": list(items)}


def ws_update(feed_id: str, value: str, publish_time: int) -> str:
    return json.dumps(
        {
            "type": "price_update",
            "price_feed": {
                "id": feed_id,
                "price": price(value, publish_time),
                "ema_price": price(value, publish_time),
                "metadata": {"emitter_chain": 0, "price_service_receive_time": publish_time, "prev_publish_time": publish_time - 1},
            },
        }
    )


async def sse_open(request: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(headers={"content-type": "text/event-stream", "cache-control": "no-cache"})
    await resp.prepare(request)
    return resp


async def sse_event(resp: web.StreamResponse, body: Any) -> None:
    await resp.write(f"data: {json.dumps(body)}\n\n".encode())


async def hold(request: web.Request) -> None:
    while request.transport is not None and not request.transport.is_closing():
        await asyncio.sleep(0.02)


class FakeAstra:
    def __init__(self) -> None:
        self.requests: list[web.Request] = []
        self.http: HttpHandler | None = None
        self.ws: WsHandler | None = None
        self.ws_connections = 0
        self.url = ""
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._runner: web.AppRunner | None = None

    async def _dispatch(self, request: web.Request) -> web.StreamResponse:
        self.requests.append(request)
        if request.path == "/ws" and self.ws is not None:
            ws = web.WebSocketResponse(autoping=True)
            await ws.prepare(request)
            self.ws_connections += 1
            await self.ws(ws, self.ws_connections)
            return ws
        if self.http is None:
            return web.Response(status=500, text="no handler")
        return await self.http(request)

    async def _start(self) -> None:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app, shutdown_timeout=0.1, max_line_size=16384)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        host, port = self._runner.addresses[0][:2]
        self.url = f"http://{host}:{port}"

    def start(self) -> None:
        self._thread.start()
        asyncio.run_coroutine_threadsafe(self._start(), self._loop).result(10)

    async def _drain(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self._drain(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)
        self._loop.close()


@pytest.fixture
def fake() -> Iterator[FakeAstra]:
    server = FakeAstra()
    server.start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
