from __future__ import annotations

import asyncio
import inspect
import json
import time
from collections.abc import Callable, Mapping
from types import TracebackType
from typing import Any, TypeVar

import httpx

from . import _transport as tr
from ._operations import AsyncOperations, SyncOperations
from ._request import Call, RequestOptions
from ._types import ResponseInfo
from .errors import AveeError, AveePaymentError, AveeValidationError
from .x402 import Payer, decode_required, finish, prepare

T = TypeVar("T")

DEFAULT_BASE_URL = "https://api.preview.avee.tech/api/v1"
PREVIEW_BASE_URL = "https://api.preview.avee.tech/api/v1"
VERSION = "0.1.2"
_USER_AGENT = f"avee-python/{VERSION}"


class _Base:
    def __init__(
        self,
        base_url: str,
        api_key: str | None,
        timeout: float,
        max_retries: int,
        max_retry_delay: float,
        max_response_bytes: int,
        headers: Mapping[str, str] | None,
        payer: Payer | None,
    ) -> None:
        self._config = tr.validate_config(base_url, api_key, timeout, max_retries, max_retry_delay, max_response_bytes, headers, payer)
        self.last_response: ResponseInfo | None = None
        """Status, request id, rate-limit headers and x402 receipt of the most recent response."""

    def _prepare(self, call: Call) -> tuple[str, bytes | None]:
        body = None if call.body is None else json.dumps(call.body, separators=(",", ":")).encode()
        return self._config.base_url + call.path(), body

    def _challenge(self, call: Call, headers: httpx.Headers, data: bytes) -> dict[str, Any]:
        raw = decode_required(headers.get("payment-required"), data)
        if raw is None:
            raise AveePaymentError("the 402 challenge is unreadable", call.operation)
        return raw

    def _record(self, call: Call, resp: httpx.Response, data: bytes | None, url: str) -> bytes:
        self.last_response = tr.response_info(call.operation, resp.status_code, resp.headers)
        if data is None:
            raise AveeValidationError(f"{call.operation}: response from {url} exceeds {self._config.max_response_bytes} bytes")
        return data


class AveeClient(_Base, SyncOperations):
    """Synchronous client for the avee DEX data API; every /api/v1 operation is a method."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
        max_retry_delay: float = 30.0,
        max_response_bytes: int = 16 * 1024 * 1024,
        headers: Mapping[str, str] | None = None,
        payer: Payer | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        super().__init__(base_url, api_key, timeout, max_retries, max_retry_delay, max_response_bytes, headers, payer)
        self._owns = http_client is None
        self._http = http_client if http_client is not None else httpx.Client(follow_redirects=False)

    def close(self) -> None:
        if self._owns:
            self._http.close()

    def __enter__(self) -> AveeClient:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        self.close()

    def _attempt(self, call: Call, url: str, body: bytes | None, signature: str | None) -> tuple[int, httpx.Headers, bytes]:
        headers = tr.request_headers(self._config, body is not None, signature, _USER_AGENT)
        deadline = time.monotonic() + self._config.timeout
        try:
            with self._http.stream(call.method, url, params=tuple(call.query), content=body, headers=headers, timeout=self._config.timeout, follow_redirects=False) as resp:
                data = tr.read_sync(resp.iter_bytes(), self._config.max_response_bytes, deadline)
                return resp.status_code, resp.headers, self._record(call, resp, data, url)
        except httpx.HTTPError as err:
            raise tr.transport_error(err, call.method, url, self._config.timeout) from err

    def _sign(self, payer: Payer, call: Call, url: str, raw: dict[str, Any]) -> str:
        context, choice = prepare(payer, call.operation, call.method, url, raw)
        try:
            signature = payer.sign(context)
        except Exception as err:
            raise AveePaymentError(f"the payer did not sign: {err}", call.operation, raw) from err
        if inspect.isawaitable(signature):
            if inspect.iscoroutine(signature):
                signature.close()
            raise AveePaymentError("an async payer needs AsyncAveeClient", call.operation, raw)
        return finish(signature, context, choice, raw)

    def _send(self, call: Call, decode: Callable[[Any], T], options: RequestOptions | None) -> T:
        url, body = self._prepare(call)
        signature: str | None = None
        attempt = 0
        while True:
            try:
                status, headers, data = self._attempt(call, url, body, signature)
            except AveeError as err:
                delay = tr.retry_delay(err, call.method, attempt, signature is not None, self._config)
                if delay is None:
                    raise
                time.sleep(delay)
                attempt += 1
                continue
            if 200 <= status < 300:
                return decode(tr.decode_json(data, call.operation, url))
            if status == 402 and self._config.payer is not None and signature is None:
                signature = self._sign(self._config.payer, call, url, self._challenge(call, headers, data))
                continue
            api_err = tr.api_error(call.operation, call.method, url, status, headers, data, signature is not None)
            delay = tr.retry_delay(api_err, call.method, attempt, signature is not None, self._config)
            if delay is None:
                raise api_err
            time.sleep(delay)
            attempt += 1


class AsyncAveeClient(_Base, AsyncOperations):
    """Asyncio client for the avee DEX data API; the same methods as coroutines."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
        max_retry_delay: float = 30.0,
        max_response_bytes: int = 16 * 1024 * 1024,
        headers: Mapping[str, str] | None = None,
        payer: Payer | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(base_url, api_key, timeout, max_retries, max_retry_delay, max_response_bytes, headers, payer)
        self._owns = http_client is None
        self._http = http_client if http_client is not None else httpx.AsyncClient(follow_redirects=False)

    async def aclose(self) -> None:
        if self._owns:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncAveeClient:
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        await self.aclose()

    async def _attempt(self, call: Call, url: str, body: bytes | None, signature: str | None) -> tuple[int, httpx.Headers, bytes]:
        headers = tr.request_headers(self._config, body is not None, signature, _USER_AGENT)
        deadline = time.monotonic() + self._config.timeout
        try:
            async with self._http.stream(call.method, url, params=tuple(call.query), content=body, headers=headers, timeout=self._config.timeout, follow_redirects=False) as resp:
                data = await tr.read_async(resp.aiter_bytes(), self._config.max_response_bytes, deadline)
                return resp.status_code, resp.headers, self._record(call, resp, data, url)
        except httpx.HTTPError as err:
            raise tr.transport_error(err, call.method, url, self._config.timeout) from err

    async def _sign(self, payer: Payer, call: Call, url: str, raw: dict[str, Any]) -> str:
        context, choice = prepare(payer, call.operation, call.method, url, raw)
        try:
            signature = payer.sign(context)
            if inspect.isawaitable(signature):
                signature = await signature
        except Exception as err:
            raise AveePaymentError(f"the payer did not sign: {err}", call.operation, raw) from err
        return finish(signature, context, choice, raw)

    async def _send(self, call: Call, decode: Callable[[Any], T], options: RequestOptions | None) -> T:
        url, body = self._prepare(call)
        signature: str | None = None
        attempt = 0
        while True:
            try:
                status, headers, data = await self._attempt(call, url, body, signature)
            except AveeError as err:
                delay = tr.retry_delay(err, call.method, attempt, signature is not None, self._config)
                if delay is None:
                    raise
                await asyncio.sleep(delay)
                attempt += 1
                continue
            if 200 <= status < 300:
                return decode(tr.decode_json(data, call.operation, url))
            if status == 402 and self._config.payer is not None and signature is None:
                signature = await self._sign(self._config.payer, call, url, self._challenge(call, headers, data))
                continue
            api_err = tr.api_error(call.operation, call.method, url, status, headers, data, signature is not None)
            delay = tr.retry_delay(api_err, call.method, attempt, signature is not None, self._config)
            if delay is None:
                raise api_err
            await asyncio.sleep(delay)
            attempt += 1
