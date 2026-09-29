from __future__ import annotations

import json
import random
import re
import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from ._types import RateLimit, ResponseInfo
from .errors import AveeAPIError, AveeConnectionError, AveeError, AveeTimeoutError, AveeValidationError, clip
from .x402 import Payer, decode_required, parse_receipt

RETRY_BASE_DELAY = 0.25
_INT = re.compile(r"^\s*\d{1,12}\s*$")
_MULTI_LINE = re.compile(r"[\r\n\0]")
_PROBLEM_STRINGS = ("type", "title", "code", "detail", "param", "request_id")


@dataclass(frozen=True, slots=True)
class Config:
    base_url: str
    api_key: str | None
    timeout: float
    max_retries: int
    max_retry_delay: float
    max_response_bytes: int
    headers: dict[str, str]
    payer: Payer | None


def validate_config(
    base_url: str,
    api_key: str | None,
    timeout: float,
    max_retries: int,
    max_retry_delay: float,
    max_response_bytes: int,
    headers: Mapping[str, str] | None,
    payer: Payer | None,
) -> Config:
    try:
        parsed = httpx.URL(base_url)
    except (TypeError, httpx.InvalidURL):
        raise AveeValidationError(f"base_url must be an absolute http(s) URL, got {base_url!r}") from None
    if parsed.scheme not in ("http", "https") or not parsed.host or parsed.query:
        raise AveeValidationError(f"base_url must be an absolute http(s) URL without a query, got {base_url!r}")
    if api_key is not None and (not isinstance(api_key, str) or not api_key or _MULTI_LINE.search(api_key)):
        raise AveeValidationError("api_key must be a single non-empty line")
    for header, text in (headers or {}).items():
        if not isinstance(header, str) or header.lower() == "payment-signature":
            raise AveeValidationError("the payment-signature header is sent only by the payer")
        if not isinstance(text, str) or _MULTI_LINE.search(text):
            raise AveeValidationError(f"header {header} must be a single-line string")
    for name, value in (("timeout", timeout), ("max_retry_delay", max_retry_delay)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0 or (name == "timeout" and value == 0):
            raise AveeValidationError(f"{name} must be a positive number")
    for name, count in (("max_retries", max_retries), ("max_response_bytes", max_response_bytes)):
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise AveeValidationError(f"{name} must be a non-negative integer")
    if payer is not None and not callable(getattr(payer, "sign", None)):
        raise AveeValidationError("payer must have a sign(context) method")
    networks = getattr(payer, "networks", None)
    if networks is not None and (isinstance(networks, (str, bytes)) or not isinstance(networks, Sequence) or not all(isinstance(n, str) for n in networks)):
        raise AveeValidationError("payer.networks must be a sequence of strings")
    return Config(base_url.rstrip("/"), api_key, float(timeout), max_retries, float(max_retry_delay), max_response_bytes, dict(headers or {}), payer)


def request_headers(config: Config, has_body: bool, signature: str | None, user_agent: str) -> dict[str, str]:
    h = {**config.headers, "accept": "application/json", "user-agent": user_agent}
    if has_body:
        h["content-type"] = "application/json"
    if config.api_key:
        h["x-api-key"] = config.api_key
    if config.payer is not None:
        h["accept-payment"] = "x402"
    if signature is not None:
        h["payment-signature"] = signature
    return h


def full_jitter(attempt: int, base: float, cap: float) -> float:
    return random.uniform(0, min(cap, base * 2 ** min(attempt, 30)))


def parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value.isdigit() and len(value) <= 9:
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _int(value: str | None) -> int | None:
    return int(value) if value is not None and _INT.match(value) else None


def parse_rate_limit(headers: Mapping[str, str]) -> RateLimit:
    fields: dict[str, str] = {}
    for part in (headers.get("ratelimit") or "").split(";"):
        k, sep, v = part.partition("=")
        if sep:
            fields[k.strip()] = v.strip()
    remaining = _int(headers.get("x-ratelimit-remaining"))
    reset = _int(headers.get("x-ratelimit-reset"))
    return RateLimit(
        limit=_int(headers.get("x-ratelimit-limit")),
        remaining=remaining if remaining is not None else _int(fields.get("r")),
        reset_seconds=reset if reset is not None else _int(fields.get("t")),
        retry_after_seconds=parse_retry_after(headers.get("retry-after")),
        policy=headers.get("ratelimit-policy"),
    )


def response_info(operation: str, status: int, headers: Mapping[str, str]) -> ResponseInfo:
    return ResponseInfo(operation, status, headers.get("x-request-id"), parse_rate_limit(headers), parse_receipt(headers))


def as_problem(text: str, content_type: str) -> Mapping[str, Any] | None:
    if "json" not in content_type:
        return None
    try:
        parsed = json.loads(text)
    except (ValueError, RecursionError):
        return None
    if not isinstance(parsed, dict):
        return None
    if any(parsed.get(k) is not None and not isinstance(parsed[k], str) for k in _PROBLEM_STRINGS):
        return None
    status = parsed.get("status")
    if status is not None and (isinstance(status, bool) or not isinstance(status, int)):
        return None
    return parsed if parsed.get("code") or parsed.get("title") else None


def api_error(operation: str, method: str, url: str, status: int, headers: Mapping[str, str], body: bytes, paid: bool) -> AveeAPIError:
    text = body.decode("utf-8", errors="replace")
    problem = as_problem(text, headers.get("content-type", ""))
    return AveeAPIError(
        status=status,
        operation=operation,
        method=method,
        url=url,
        body=clip(text, 64 * 1024),
        headers=headers,
        problem=problem,
        rate_limit=parse_rate_limit(headers),
        retry_after=parse_retry_after(headers.get("retry-after")),
        payment_required=decode_required(headers.get("payment-required"), body) if status == 402 else None,
        payment=parse_receipt(headers),
        paid=paid,
    )


def retry_delay(err: AveeError, method: str, attempt: int, paid: bool, config: Config) -> float | None:
    if paid or method != "GET" or attempt >= config.max_retries:
        return None
    backoff = full_jitter(attempt, RETRY_BASE_DELAY, config.max_retry_delay)
    if isinstance(err, AveeAPIError):
        if not err.retryable:
            return None
        wait = err.retry_after
        if wait is None and err.status == 429 and err.rate_limit.remaining == 0 and err.rate_limit.reset_seconds is not None:
            wait = float(err.rate_limit.reset_seconds)
        if not wait:
            return backoff
        return wait if wait <= config.max_retry_delay else None
    if isinstance(err, (AveeTimeoutError, AveeConnectionError)):
        return backoff
    return None


def _append(buf: bytearray, chunk: bytes, limit: int, deadline: float) -> bool:
    if time.monotonic() > deadline:
        raise httpx.ReadTimeout("the response took longer than the timeout")
    buf += chunk
    return len(buf) <= limit


def read_sync(chunks: Iterator[bytes], limit: int, deadline: float) -> bytes | None:
    buf = bytearray()
    for chunk in chunks:
        if not _append(buf, chunk, limit, deadline):
            return None
    return bytes(buf)


async def read_async(chunks: AsyncIterator[bytes], limit: int, deadline: float) -> bytes | None:
    buf = bytearray()
    async for chunk in chunks:
        if not _append(buf, chunk, limit, deadline):
            return None
    return bytes(buf)


def decode_json(body: bytes, operation: str, url: str) -> Any:
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        raise AveeValidationError(f"{operation}: response from {url} is not JSON") from None


def transport_error(err: Exception, method: str, url: str, timeout: float) -> AveeError:
    if isinstance(err, httpx.TimeoutException):
        return AveeTimeoutError(f"{method} {url} timed out after {timeout} s")
    return AveeConnectionError(f"{method} {url} failed: {err}")

