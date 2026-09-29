from __future__ import annotations

import json
import random
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Generic, TypeVar

from .errors import (
    AstraConnectionError,
    AstraError,
    AstraHTTPError,
    AstraTimeoutError,
    AstraValidationError,
    Problem,
    clip,
)

T = TypeVar("T")

ERROR_BODY_LIMIT = 64 * 1024
RETRY_BASE_DELAY = 0.25


@dataclass(frozen=True, slots=True)
class Call(Generic[T]):
    path: str
    params: list[tuple[str, str]]
    decode: Callable[[Any], T]
    accept: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class Config:
    base_url: str
    timeout: float
    max_retries: int
    max_retry_delay: float
    max_response_bytes: int
    headers: dict[str, str]

    def url(self, path: str) -> str:
        return self.base_url.rstrip("/") + "/" + path.lstrip("/")


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


def http_error(status: int, url: str, headers: Mapping[str, str], body: bytes) -> AstraHTTPError:
    text = body.decode("utf-8", errors="replace")
    detail = clip(text.strip())
    problem = None
    if "json" in headers.get("content-type", ""):
        try:
            parsed = json.loads(text)
        except (ValueError, RecursionError):
            parsed = None
        if isinstance(parsed, dict):
            title, code = parsed.get("title"), parsed.get("status")
            if isinstance(title, str) and isinstance(code, int) and not isinstance(code, bool):
                kind, text_detail = parsed.get("type"), parsed.get("detail")
                problem = Problem(
                    type=kind if isinstance(kind, str) else "about:blank",
                    title=title,
                    status=code,
                    detail=text_detail if isinstance(text_detail, str) else "",
                )
                detail = clip(problem.detail or problem.title)
            elif isinstance(parsed.get("errmsg"), str):
                detail = clip(parsed["errmsg"])
    return AstraHTTPError(
        status,
        url,
        text,
        detail=detail,
        problem=problem,
        retry_after=parse_retry_after(headers.get("retry-after")),
    )


def retry_delay(err: AstraError, attempt: int, config: Config) -> float | None:
    if attempt >= config.max_retries:
        return None
    backoff = full_jitter(attempt, RETRY_BASE_DELAY, config.max_retry_delay)
    if isinstance(err, AstraHTTPError):
        if not err.retryable:
            return None
        if err.retry_after is None:
            return backoff
        return err.retry_after if err.retry_after <= config.max_retry_delay else None
    if isinstance(err, (AstraTimeoutError, AstraConnectionError)):
        return backoff
    return None


class Accumulator:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.buf = bytearray()
        self.truncated = False

    def add(self, chunk: bytes) -> bool:
        room = self.limit - len(self.buf)
        if len(chunk) > room:
            self.buf += chunk[:room]
            self.truncated = True
            return False
        self.buf += chunk
        return True


def read_sync(chunks: Iterator[bytes], limit: int) -> Accumulator:
    acc = Accumulator(limit)
    for chunk in chunks:
        if not acc.add(chunk):
            break
    return acc


async def read_async(chunks: AsyncIterator[bytes], limit: int) -> Accumulator:
    acc = Accumulator(limit)
    async for chunk in chunks:
        if not acc.add(chunk):
            break
    return acc


def parse_json(acc: Accumulator, url: str, limit: int) -> Any:
    if acc.truncated:
        raise AstraValidationError(f"response from {url} exceeds {limit} bytes")
    try:
        return json.loads(acc.buf)
    except (ValueError, RecursionError):
        raise AstraValidationError(f"response from {url} is not JSON") from None
