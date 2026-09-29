from __future__ import annotations

import asyncio
import itertools
import math
import re
import time
from collections.abc import Callable, Iterable
from typing import Any, TypeVar

import httpx

from . import models
from ._transport import ERROR_BODY_LIMIT, Call, Config, http_error, parse_json, read_async, read_sync, retry_delay
from .errors import AstraConnectionError, AstraError, AstraTimeoutError, AstraValidationError
from .ids import MAX_HISTORICAL_FEEDS, MAX_IDS_PER_REQUEST, MAX_IDS_PER_URL, normalize_feed_id, normalize_feed_ids, unique_feed_ids
from .models import Candle, Channel, Feed, FeedIdEntry, FeedIdMap, FeedMetadata, PriceUpdate, StatusReport
from .stream import ConnectionState, SseTransport, Subscription, WsTransport

DEFAULT_BASE_URL = "https://astra.preview.avee.tech"
MAX_INTERVAL_SECONDS = 60
FEED_IDS_PER_REQUEST = MAX_IDS_PER_URL
_MAX_UNIX_SECONDS = 100_000_000_000
_RESOLUTION = re.compile(r"[0-9A-Za-z]{1,8}")

T = TypeVar("T")


def _positive(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise AstraValidationError(f"{name} must be a positive finite number, got {value!r}")
    return float(value)


def _count(name: str, value: int, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or int(value) < minimum:
        raise AstraValidationError(f"{name} must be an integer of at least {minimum}, got {value!r}")
    return int(value)


def _config(
    base_url: str,
    timeout: float,
    max_retries: int,
    max_retry_delay: float,
    max_response_bytes: int,
    headers: dict[str, str] | None,
) -> Config:
    try:
        parsed = httpx.URL(base_url)
    except (TypeError, httpx.InvalidURL):
        raise AstraValidationError(f"base_url must be an absolute http(s) URL, got {base_url!r}") from None
    if parsed.scheme not in ("http", "https") or not parsed.host:
        raise AstraValidationError(f"base_url must be an absolute http(s) URL, got {base_url!r}")
    return Config(
        base_url,
        _positive("timeout", timeout),
        _count("max_retries", max_retries, 0),
        _positive("max_retry_delay", max_retry_delay),
        _count("max_response_bytes", max_response_bytes, 1),
        dict(headers or {}),
    )


def _unix(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_UNIX_SECONDS:
        raise AstraValidationError(f"{name} must be a non-negative integer of Unix seconds, got {value!r}")
    return value


def _ids(ids: Iterable[str], limit: int) -> list[tuple[str, str]]:
    return [("ids[]", i) for i in normalize_feed_ids(ids, limit)]


def _flag(name: str, value: bool | None) -> list[tuple[str, str]]:
    return [] if value is None else [(name, "true" if value else "false")]


def _price_feeds(query: str | None, asset_type: str | None) -> Call[list[FeedMetadata]]:
    params = [("query", query)] if query else []
    params += [("asset_type", asset_type)] if asset_type else []
    return Call("v2/price_feeds", params, lambda b: models.decode_list(b, "price_feeds", models.decode_feed_metadata))


def _price_feed(feed_id: str) -> Call[FeedMetadata]:
    return Call(f"v2/price_feeds/{normalize_feed_id(feed_id)}", [], lambda b: models.decode_feed_metadata(b, "price_feed"))


def _envelope(body: Any) -> list[PriceUpdate]:
    return models.decode_update_envelope(body, "update")


def _latest(ids: Iterable[str], ignore_invalid: bool | None) -> list[Call[list[PriceUpdate]]]:
    params = _ids(ids, MAX_IDS_PER_REQUEST)
    flags = _flag("ignore_invalid_price_ids", ignore_invalid)
    return [
        Call("v2/updates/price/latest", params[start : start + MAX_IDS_PER_URL] + flags, _envelope)
        for start in range(0, len(params), MAX_IDS_PER_URL)
    ]


def _at(publish_time: int, ids: Iterable[str], ignore_invalid: bool | None) -> Call[list[PriceUpdate]]:
    t = _unix("publish_time", publish_time)
    return Call(f"v2/updates/price/{t}", _ids(ids, MAX_HISTORICAL_FEEDS) + _flag("ignore_invalid_price_ids", ignore_invalid), _envelope)


def _interval(
    publish_time: int, interval_seconds: int, ids: Iterable[str], unique: bool | None, ignore_invalid: bool | None
) -> Call[list[PriceUpdate]]:
    t = _unix("publish_time", publish_time)
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int) or not 0 <= interval_seconds <= MAX_INTERVAL_SECONDS:
        raise AstraValidationError(f"interval_seconds must be an integer in [0, {MAX_INTERVAL_SECONDS}]")
    params = _ids(ids, MAX_HISTORICAL_FEEDS) + _flag("ignore_invalid_price_ids", ignore_invalid) + _flag("unique", unique)

    def decode(body: Any) -> list[PriceUpdate]:
        return [u for batch in models.decode_list(body, "updates", models.decode_update_envelope) for u in batch]

    return Call(f"v2/updates/price/{t}/{interval_seconds}", params, decode)


def _feeds(category: str | None) -> Call[list[Feed]]:
    return Call("v1/feeds", [("category", category)] if category else [], lambda b: models.decode_list(b, "feeds", models.decode_feed))


def _feed_id_call(params: list[tuple[str, str]]) -> Call[FeedIdMap]:
    return Call("v1/feed-ids", params, lambda b: models.decode_feed_id_map(b, "feed_ids"))


def _feed_id_calls(pyth_ids: Iterable[str] | None, astra_ids: Iterable[str] | None, category: str | None) -> list[Call[FeedIdMap]]:
    tail = [("category", category)] if category else []
    if pyth_ids is None and astra_ids is None:
        return [_feed_id_call(tail)]
    pyth = [] if pyth_ids is None else unique_feed_ids(pyth_ids)
    astra = [] if astra_ids is None else unique_feed_ids(astra_ids)
    calls = []
    while pyth or astra:
        np = min(len(pyth), MAX_IDS_PER_URL)
        na = min(len(astra), MAX_IDS_PER_URL - np)
        params = [(name, ",".join(ids)) for name, ids in (("pyth_ids", pyth[:np]), ("astra_ids", astra[:na])) if ids]
        calls.append(_feed_id_call(params + tail))
        pyth, astra = pyth[np:], astra[na:]
    return calls


def _merge_feed_ids(pages: list[FeedIdMap], filtered: bool) -> FeedIdMap:
    if not filtered:
        return pages[0]
    items: dict[str, FeedIdEntry] = {}
    missing: dict[str, None] = {}
    for page in pages:
        for item in page.items:
            items.setdefault(item.astra_id, item)
        missing.update(dict.fromkeys(page.missing or []))
    return FeedIdMap(items=sorted(items.values(), key=lambda e: e.symbol), missing=list(missing))


def _status(feed: str | None) -> Call[StatusReport]:
    if feed is None or feed == "":
        return Call("v1/status", [], lambda b: models.decode_status_report(b, "status"))
    return Call("v1/status", [("feed", normalize_feed_id(feed))], lambda b: models.decode_status_report(b, "status"), accept=(503,))


def _candles(feed: str, resolution: str, from_time: int, to_time: int) -> Call[list[Candle]]:
    if not isinstance(feed, str) or not 0 < len(feed) <= 200:
        raise AstraValidationError("feed must be a feed id or symbol")
    if not isinstance(resolution, str) or not _RESOLUTION.fullmatch(resolution):
        raise AstraValidationError(f"invalid resolution {resolution!r}")
    start, end = _unix("from_time", from_time), _unix("to_time", to_time)
    if end < start:
        raise AstraValidationError("to_time must not be before from_time")
    params = [("feed", feed), ("resolution", resolution), ("from", str(start)), ("to", str(end))]
    return Call("v1/candles", params, lambda b: models.decode_candles(b, "candles"))


def _accepted(resp: httpx.Response, accept: tuple[int, ...]) -> bool:
    media_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
    return resp.status_code in accept and media_type == "application/json"


def _wrap_transport_error(err: httpx.HTTPError, url: str, timeout: float) -> AstraError:
    if isinstance(err, httpx.TimeoutException):
        return AstraTimeoutError(f"request to {url} timed out after {timeout} s")
    return AstraConnectionError(f"request to {url} failed: {err}")


class AstraClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout: float = 10.0,
        max_retries: int = 2,
        max_retry_delay: float = 30.0,
        max_response_bytes: int = 8 * 1024 * 1024,
        headers: dict[str, str] | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._config = _config(base_url, timeout, max_retries, max_retry_delay, max_response_bytes, headers)
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client()

    def __enter__(self) -> AstraClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def price_feeds(self, *, query: str | None = None, asset_type: str | None = None) -> list[FeedMetadata]:
        return self._send(_price_feeds(query, asset_type))

    def price_feed(self, feed_id: str) -> FeedMetadata:
        return self._send(_price_feed(feed_id))

    def latest_prices(self, ids: Iterable[str], *, ignore_invalid: bool | None = None) -> list[PriceUpdate]:
        return [u for call in _latest(ids, ignore_invalid) for u in self._send(call)]

    def prices_at(self, publish_time: int, ids: Iterable[str], *, ignore_invalid: bool | None = None) -> list[PriceUpdate]:
        return self._send(_at(publish_time, ids, ignore_invalid))

    def prices_in_interval(
        self,
        publish_time: int,
        interval_seconds: int,
        ids: Iterable[str],
        *,
        unique: bool | None = None,
        ignore_invalid: bool | None = None,
    ) -> list[PriceUpdate]:
        return self._send(_interval(publish_time, interval_seconds, ids, unique, ignore_invalid))

    def feeds(self, *, category: str | None = None) -> list[Feed]:
        return self._send(_feeds(category))

    def feed_ids(
        self,
        pyth_ids: Iterable[str] | None = None,
        astra_ids: Iterable[str] | None = None,
        *,
        category: str | None = None,
    ) -> FeedIdMap:
        calls = _feed_id_calls(pyth_ids, astra_ids, category)
        return _merge_feed_ids([self._send(c) for c in calls], pyth_ids is not None or astra_ids is not None)

    def status(self, *, feed: str | None = None) -> StatusReport:
        return self._send(_status(feed))

    def candles(self, feed: str, resolution: str, from_time: int, to_time: int) -> list[Candle]:
        return self._send(_candles(feed, resolution, from_time, to_time))

    def _send(self, call: Call[T]) -> T:
        url = self._config.url(call.path)
        for attempt in itertools.count():
            try:
                return call.decode(self._get(url, call.params, call.accept))
            except AstraError as err:
                delay = retry_delay(err, attempt, self._config)
                if delay is None:
                    raise
                time.sleep(delay)
        raise AssertionError("unreachable")

    def _get(self, url: str, params: list[tuple[str, str]], accept: tuple[int, ...] = ()) -> Any:
        cfg = self._config
        headers = {"accept": "application/json", **cfg.headers}
        try:
            with self._http.stream("GET", url, params=tuple(params), headers=headers, timeout=cfg.timeout) as resp:
                if resp.status_code != 200 and not _accepted(resp, accept):
                    body = read_sync(resp.iter_bytes(), ERROR_BODY_LIMIT).buf
                    raise http_error(resp.status_code, str(resp.request.url), resp.headers, bytes(body))
                acc = read_sync(resp.iter_bytes(), cfg.max_response_bytes)
        except httpx.HTTPError as err:
            raise _wrap_transport_error(err, url, cfg.timeout) from err
        return parse_json(acc, url, cfg.max_response_bytes)


class AsyncAstraClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout: float = 10.0,
        max_retries: int = 2,
        max_retry_delay: float = 30.0,
        max_response_bytes: int = 8 * 1024 * 1024,
        headers: dict[str, str] | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = _config(base_url, timeout, max_retries, max_retry_delay, max_response_bytes, headers)
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient()

    async def __aenter__(self) -> AsyncAstraClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def price_feeds(self, *, query: str | None = None, asset_type: str | None = None) -> list[FeedMetadata]:
        return await self._send(_price_feeds(query, asset_type))

    async def price_feed(self, feed_id: str) -> FeedMetadata:
        return await self._send(_price_feed(feed_id))

    async def latest_prices(self, ids: Iterable[str], *, ignore_invalid: bool | None = None) -> list[PriceUpdate]:
        return [u for call in _latest(ids, ignore_invalid) for u in await self._send(call)]

    async def prices_at(self, publish_time: int, ids: Iterable[str], *, ignore_invalid: bool | None = None) -> list[PriceUpdate]:
        return await self._send(_at(publish_time, ids, ignore_invalid))

    async def prices_in_interval(
        self,
        publish_time: int,
        interval_seconds: int,
        ids: Iterable[str],
        *,
        unique: bool | None = None,
        ignore_invalid: bool | None = None,
    ) -> list[PriceUpdate]:
        return await self._send(_interval(publish_time, interval_seconds, ids, unique, ignore_invalid))

    async def feeds(self, *, category: str | None = None) -> list[Feed]:
        return await self._send(_feeds(category))

    async def feed_ids(
        self,
        pyth_ids: Iterable[str] | None = None,
        astra_ids: Iterable[str] | None = None,
        *,
        category: str | None = None,
    ) -> FeedIdMap:
        calls = _feed_id_calls(pyth_ids, astra_ids, category)
        return _merge_feed_ids([await self._send(c) for c in calls], pyth_ids is not None or astra_ids is not None)

    async def status(self, *, feed: str | None = None) -> StatusReport:
        return await self._send(_status(feed))

    async def candles(self, feed: str, resolution: str, from_time: int, to_time: int) -> list[Candle]:
        return await self._send(_candles(feed, resolution, from_time, to_time))

    def subscribe(
        self,
        ids: Iterable[str],
        *,
        transport: str = "ws",
        channel: Channel | None = None,
        ignore_invalid: bool = False,
        benchmarks_only: bool = False,
        idle_timeout: float = 45.0,
        max_message_bytes: int = 1024 * 1024,
        reconnect_base_delay: float = 0.5,
        reconnect_max_delay: float = 30.0,
        stable_after: float = 60.0,
        on_error: Callable[[AstraError], None] | None = None,
        on_state_change: Callable[[ConnectionState], None] | None = None,
    ) -> Subscription:
        feed_ids = normalize_feed_ids(ids, MAX_IDS_PER_REQUEST)
        idle_timeout = _positive("idle_timeout", idle_timeout)
        max_message_bytes = _count("max_message_bytes", max_message_bytes, 1)
        reconnect_base_delay = _positive("reconnect_base_delay", reconnect_base_delay)
        reconnect_max_delay = _positive("reconnect_max_delay", reconnect_max_delay)
        stable_after = _positive("stable_after", stable_after)
        headers = self._config.headers
        stream: WsTransport | SseTransport
        if transport == "ws":
            if benchmarks_only:
                raise AstraValidationError("benchmarks_only is supported on the SSE transport only")
            url = httpx.URL(self._config.url("ws"))
            url = url.copy_with(scheme="wss" if url.scheme == "https" else "ws")
            if channel:
                url = url.copy_merge_params({"channel": channel})
            stream = WsTransport(str(url), feed_ids, ignore_invalid, idle_timeout, max_message_bytes, headers)
        elif transport == "sse":
            if len(feed_ids) > MAX_IDS_PER_URL:
                raise AstraValidationError(
                    f'SSE carries feed ids in the URL: at most {MAX_IDS_PER_URL}, got {len(feed_ids)}; use transport "ws" for more'
                )
            params = [("ids[]", i) for i in feed_ids]
            params += [("channel", channel)] if channel else []
            params += _flag("ignore_invalid_price_ids", ignore_invalid or None)
            params += _flag("benchmarks_only", benchmarks_only or None)
            url = httpx.URL(self._config.url("v2/updates/price/stream"), params=params)
            stream = SseTransport(self._http, str(url), idle_timeout, max_message_bytes, headers)
        else:
            raise AstraValidationError(f"unknown transport {transport!r}")
        return Subscription(
            feed_ids,
            stream,
            base_delay=reconnect_base_delay,
            max_delay=reconnect_max_delay,
            stable_after=stable_after,
            on_error=on_error,
            on_state_change=on_state_change,
        )

    async def _send(self, call: Call[T]) -> T:
        url = self._config.url(call.path)
        for attempt in itertools.count():
            try:
                return call.decode(await self._get(url, call.params, call.accept))
            except AstraError as err:
                delay = retry_delay(err, attempt, self._config)
                if delay is None:
                    raise
                await asyncio.sleep(delay)
        raise AssertionError("unreachable")

    async def _get(self, url: str, params: list[tuple[str, str]], accept: tuple[int, ...] = ()) -> Any:
        cfg = self._config
        headers = {"accept": "application/json", **cfg.headers}
        try:
            async with self._http.stream("GET", url, params=tuple(params), headers=headers, timeout=cfg.timeout) as resp:
                if resp.status_code != 200 and not _accepted(resp, accept):
                    body = (await read_async(resp.aiter_bytes(), ERROR_BODY_LIMIT)).buf
                    raise http_error(resp.status_code, str(resp.request.url), resp.headers, bytes(body))
                acc = await read_async(resp.aiter_bytes(), cfg.max_response_bytes)
        except httpx.HTTPError as err:
            raise _wrap_transport_error(err, url, cfg.timeout) from err
        return parse_json(acc, url, cfg.max_response_bytes)
