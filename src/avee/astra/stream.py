from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

import certifi
import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidHandshake, InvalidStatus

from ._transport import ERROR_BODY_LIMIT, full_jitter, http_error, read_async
from .errors import (
    AstraConnectionError,
    AstraError,
    AstraHTTPError,
    AstraSubscriptionError,
    AstraTimeoutError,
    AstraValidationError,
    clip,
)
from .models import PriceUpdate, decode_streamed_price_feed, decode_update_envelope

ConnectionState = Literal["connecting", "open", "reconnecting", "closed"]

_log = logging.getLogger(__name__)
_HEARTBEAT_FACTOR = 0.5


@dataclass(slots=True)
class SubscriptionStats:
    connects: int = 0
    reconnects: int = 0
    coalesced: int = 0
    duplicates: int = 0
    invalid: int = 0


class _Sink(Protocol):
    def opened(self) -> None: ...
    def update(self, update: PriceUpdate) -> None: ...
    def warn(self, err: AstraError) -> None: ...
    def report(self, err: AstraError) -> None: ...


class _Transport(Protocol):
    async def run(self, sink: _Sink) -> None: ...


class SseParser:
    def __init__(self, max_bytes: int, on_data: Callable[[bytes], None]) -> None:
        self._max = max_bytes
        self._on_data = on_data
        self._buf = bytearray()
        self._data: list[bytes] = []
        self._size = 0

    def feed(self, chunk: bytes) -> None:
        self._buf += chunk
        start = 0
        while (nl := self._buf.find(b"\n", start)) >= 0:
            end = nl - 1 if nl > start and self._buf[nl - 1] == 13 else nl
            self._line(bytes(self._buf[start:end]))
            start = nl + 1
        del self._buf[:start]
        if len(self._buf) > self._max:
            raise AstraConnectionError(f"SSE line exceeds {self._max} bytes")

    def _line(self, line: bytes) -> None:
        if not line:
            if self._data:
                payload = b"\n".join(self._data)
                self._data = []
                self._size = 0
                self._on_data(payload)
            return
        if line[0] == 58:
            return
        field, _, value = line.partition(b":")
        if field != b"data":
            return
        if value.startswith(b" "):
            value = value[1:]
        self._size += len(value)
        if self._size > self._max:
            raise AstraConnectionError(f"SSE event exceeds {self._max} bytes")
        self._data.append(value)


class SseTransport:
    def __init__(self, http: httpx.AsyncClient, url: str, idle_timeout: float, max_bytes: int, headers: dict[str, str]) -> None:
        self._http = http
        self._url = url
        self._idle = idle_timeout
        self._max = max_bytes
        self._headers = {"accept": "text/event-stream", "cache-control": "no-cache", **headers}

    async def run(self, sink: _Sink) -> None:
        parser = SseParser(self._max, lambda data: _dispatch_envelope(data, sink))
        try:
            async with self._http.stream("GET", self._url, headers=self._headers, timeout=httpx.Timeout(self._idle)) as resp:
                if resp.status_code != 200:
                    body = (await read_async(resp.aiter_bytes(), ERROR_BODY_LIMIT)).buf
                    raise http_error(resp.status_code, str(resp.request.url), resp.headers, bytes(body))
                sink.opened()
                async for chunk in resp.aiter_bytes():
                    parser.feed(chunk)
        except httpx.TimeoutException:
            raise AstraTimeoutError(f"SSE stream idle for {self._idle} s") from None
        except httpx.HTTPError as err:
            raise AstraConnectionError(f"SSE stream failed: {err}") from err


def _dispatch_envelope(data: bytes, sink: _Sink) -> None:
    try:
        body = json.loads(data)
    except (ValueError, RecursionError):
        sink.warn(AstraValidationError("SSE event is not JSON"))
        return
    try:
        updates = decode_update_envelope(body, "event")
    except AstraValidationError as err:
        sink.warn(err)
        return
    for u in updates:
        sink.update(u)


class WsTransport:
    def __init__(
        self, url: str, ids: list[str], ignore_invalid: bool, idle_timeout: float, max_bytes: int, headers: dict[str, str]
    ) -> None:
        self._url = url
        self._idle = idle_timeout
        self._max = max_bytes
        self._headers = headers
        self._ssl = ssl.create_default_context(cafile=certifi.where()) if url.startswith("wss:") else None
        self._subscribe = json.dumps(
            {"type": "subscribe", "ids": ids, "verbose": True, "binary": False, "ignore_invalid_price_ids": ignore_invalid}
        )

    async def run(self, sink: _Sink) -> None:
        interval = self._idle * _HEARTBEAT_FACTOR
        try:
            async with connect(
                self._url,
                additional_headers=self._headers,
                max_size=self._max,
                ping_interval=interval,
                ping_timeout=interval,
                open_timeout=self._idle,
                close_timeout=2,
                ssl=self._ssl,
            ) as ws:
                await ws.send(self._subscribe)
                awaiting_ack = True
                deadline = asyncio.get_running_loop().time() + self._idle
                while awaiting_ack:
                    remaining = deadline - asyncio.get_running_loop().time()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), max(remaining, 0))
                    except (TimeoutError, asyncio.TimeoutError):
                        raise AstraTimeoutError(f"no subscription response within {self._idle} s") from None
                    if isinstance(raw, str):
                        awaiting_ack = _handle_ws_message(raw, awaiting_ack, sink)
                try:
                    async for raw in ws:
                        if isinstance(raw, str):
                            _handle_ws_message(raw, False, sink)
                except asyncio.CancelledError:
                    await ws.close()
                    raise
        except InvalidStatus as err:
            resp = err.response
            raise http_error(resp.status_code, self._url, resp.headers, bytes(resp.body or b"")[:ERROR_BODY_LIMIT]) from None
        except ConnectionClosedOK:
            return
        except ConnectionClosedError as err:
            if err.rcvd is None and err.sent is not None and err.sent.code == 1011:
                raise AstraTimeoutError(f"WebSocket ping unanswered within {interval} s") from None
            code = err.rcvd.code if err.rcvd else 1006
            reason = err.rcvd.reason if err.rcvd else ""
            raise AstraConnectionError(f"WebSocket closed with code {code}" + (f": {clip(reason)}" if reason else "")) from None
        except (TimeoutError, asyncio.TimeoutError):
            raise AstraTimeoutError(f"WebSocket to {self._url} timed out") from None
        except (OSError, InvalidHandshake) as err:
            raise AstraConnectionError(f"WebSocket to {self._url} failed: {err}") from err


def _handle_ws_message(raw: str, awaiting_ack: bool, sink: _Sink) -> bool:
    try:
        msg = json.loads(raw)
    except (ValueError, RecursionError):
        sink.warn(AstraValidationError("WebSocket message is not JSON"))
        return awaiting_ack
    if not isinstance(msg, dict):
        return awaiting_ack
    kind = msg.get("type")
    if kind == "response":
        failed = msg.get("status") == "error"
        text = clip(msg["error"]) if isinstance(msg.get("error"), str) else "unknown error"
        if awaiting_ack:
            if failed:
                raise AstraSubscriptionError(f"Astra refused the subscription: {text}")
            sink.opened()
            return False
        if failed:
            sink.report(AstraError(f"Astra: {text}"))
    elif kind == "price_update":
        try:
            sink.update(decode_streamed_price_feed(msg.get("price_feed"), "price_feed"))
        except AstraValidationError as err:
            sink.warn(err)
    return awaiting_ack


def _as_astra_error(exc: Exception) -> AstraError:
    if isinstance(exc, AstraError):
        return exc
    err = AstraConnectionError(f"stream failed: {clip(repr(exc))}")
    err.__cause__ = exc
    return err


def _fatal(err: AstraError) -> bool:
    if isinstance(err, AstraSubscriptionError):
        return True
    return isinstance(err, AstraHTTPError) and not err.retryable


class Subscription:
    def __init__(
        self,
        ids: list[str],
        transport: _Transport,
        *,
        base_delay: float,
        max_delay: float,
        stable_after: float,
        on_error: Callable[[AstraError], None] | None,
        on_state_change: Callable[[ConnectionState], None] | None,
    ) -> None:
        self.ids: tuple[str, ...] = tuple(ids)
        self._wanted = frozenset(ids)
        self._transport = transport
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._stable_after = stable_after
        self._on_error = on_error
        self._on_state_change = on_state_change
        self._pending: OrderedDict[str, PriceUpdate] = OrderedDict()
        self._last: dict[str, PriceUpdate] = {}
        self._wakeup = asyncio.Event()
        self._error: AstraError | None = None
        self._finished = False
        self._state: ConnectionState = "connecting"
        self._opened_at = 0.0
        self.stats = SubscriptionStats()
        self._task = asyncio.get_running_loop().create_task(self._run())

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def error(self) -> AstraError | None:
        return self._error

    def __aiter__(self) -> Subscription:
        return self

    async def __anext__(self) -> PriceUpdate:
        while True:
            if self._pending:
                return self._pending.popitem(last=False)[1]
            if self._finished:
                if self._error is not None:
                    raise self._error
                raise StopAsyncIteration
            self._wakeup.clear()
            await self._wakeup.wait()

    async def __aenter__(self) -> Subscription:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            cancelling = getattr(current, "cancelling", None)
            if cancelling is not None and cancelling():
                raise

    async def wait_closed(self) -> None:
        await asyncio.shield(self._task)

    def _set_state(self, state: ConnectionState) -> None:
        if state == self._state:
            return
        self._state = state
        if self._on_state_change is not None:
            try:
                self._on_state_change(state)
            except Exception:
                _log.exception("on_state_change callback failed")

    def report(self, err: AstraError) -> None:
        if self._on_error is not None:
            try:
                self._on_error(err)
            except Exception:
                _log.exception("on_error callback failed")

    def warn(self, err: AstraError) -> None:
        self.stats.invalid += 1
        self.report(err)

    def opened(self) -> None:
        self._opened_at = time.monotonic()
        self.stats.connects += 1
        self._set_state("open")

    def update(self, u: PriceUpdate) -> None:
        if u.id not in self._wanted:
            return
        prev = self._last.get(u.id)
        if prev is not None and (
            u.price.publish_time < prev.price.publish_time
            or (
                u.price.publish_time == prev.price.publish_time
                and u.price.price == prev.price.price
                and u.price.conf == prev.price.conf
                and u.ema_price.price == prev.ema_price.price
            )
        ):
            self.stats.duplicates += 1
            return
        self._last[u.id] = u
        if u.id in self._pending:
            self.stats.coalesced += 1
        self._pending[u.id] = u
        self._wakeup.set()

    async def _run(self) -> None:
        attempt = 0
        try:
            while True:
                self._opened_at = 0.0
                retry_after = 0.0
                try:
                    await self._transport.run(self)
                except Exception as exc:
                    err = _as_astra_error(exc)
                    if _fatal(err):
                        self._error = err
                        self.report(err)
                        return
                    self.report(err)
                    if isinstance(err, AstraHTTPError) and err.retry_after is not None:
                        retry_after = err.retry_after
                if self._opened_at and time.monotonic() - self._opened_at >= self._stable_after:
                    attempt = 0
                delay = max(retry_after, full_jitter(attempt, self._base_delay, self._max_delay))
                attempt += 1
                self.stats.reconnects += 1
                self._set_state("reconnecting")
                await asyncio.sleep(delay)
        finally:
            self._finished = True
            self._set_state("closed")
            self._wakeup.set()
