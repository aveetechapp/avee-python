from __future__ import annotations

import dataclasses
import math
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NoReturn, Protocol, TypeVar
from urllib.parse import quote

from .errors import AveeValidationError

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


@dataclass(frozen=True, slots=True)
class RequestOptions:
    """Per-call options: ``max_pages`` caps an ``iter_*`` walk."""

    max_pages: int | None = None


def _range(lo: float | None, hi: float | None) -> str:
    return f"{lo if lo is not None else 0} to {hi if hi is not None else 'any'}"


class Call:
    __slots__ = ("operation", "method", "segments", "query", "body")

    def __init__(self, operation: str, method: str) -> None:
        self.operation = operation
        self.method = method
        self.segments: list[str] = []
        self.query: list[tuple[str, str]] = []
        self.body: Any = None

    def fail(self, message: str) -> NoReturn:
        raise AveeValidationError(f"{self.operation}: {message}")

    def path(self) -> str:
        return "/" + "/".join(quote(s, safe="") for s in self.segments)

    def path_literal(self, segment: str) -> None:
        self.segments.append(segment)

    def _length(self, name: str, value: str, lo: int | None, hi: int | None, uuid: bool) -> None:
        if (lo is not None and len(value) < lo) or (hi is not None and len(value) > hi):
            self.fail(f"{name} must be {_range(lo, hi)} characters, got {len(value)}")
        if uuid and not _UUID.match(value):
            self.fail(f"{name} must be a UUID")

    def path_param(self, name: str, value: str | int, *, lo: int | None = None, hi: int | None = None, uuid: bool = False) -> None:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            self.fail(f"{name} must be a string")
        if isinstance(value, int):
            if value < 0:
                self.fail(f"{name} must be a non-negative integer")
            value = str(value)
        v = value.strip()
        if v in ("", ".", ".."):
            self.fail(f"{name} must not be empty")
        self._length(name, v, lo, hi, uuid)
        self.segments.append(v)

    def _missing(self, name: str, value: Any, required: bool) -> bool:
        if value is not None:
            return False
        if required:
            self.fail(f"{name} is required")
        return True

    def q_str(self, name: str, value: str | None, *, required: bool = False, lo: int | None = None, hi: int | None = None, uuid: bool = False) -> None:
        if self._missing(name, value, required):
            return
        if not isinstance(value, str) or (required and value == ""):
            self.fail(f"{name} must be a non-empty string")
            return
        self._length(name, value, lo, hi, uuid)
        self.query.append((name, value))

    def q_int(self, name: str, value: int | None, *, required: bool = False, lo: int | None = None, hi: int | None = None) -> None:
        if self._missing(name, value, required):
            return
        if isinstance(value, bool) or not isinstance(value, int):
            self.fail(f"{name} must be an integer")
            return
        if (lo is not None and value < lo) or (hi is not None and value > hi):
            self.fail(f"{name} is out of range: {value}")
        self.query.append((name, str(value)))

    def q_float(self, name: str, value: float | None, *, required: bool = False, lo: float | None = None, hi: float | None = None) -> None:
        if self._missing(name, value, required):
            return
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            self.fail(f"{name} must be a finite number")
            return
        if (lo is not None and value < lo) or (hi is not None and value > hi):
            self.fail(f"{name} is out of range: {value}")
        self.query.append((name, repr(float(value)) if isinstance(value, float) else str(value)))

    def q_bool(self, name: str, value: bool | None, *, required: bool = False) -> None:
        if self._missing(name, value, required):
            return
        if not isinstance(value, bool):
            self.fail(f"{name} must be a boolean")
        self.query.append((name, "true" if value else "false"))

    def q_list(
        self,
        name: str,
        values: Sequence[str] | None,
        *,
        required: bool = False,
        lo: int | None = None,
        hi: int | None = None,
        item_lo: int | None = None,
        item_hi: int | None = None,
    ) -> None:
        if self._missing(name, values, required):
            return
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            self.fail(f"{name} must be a list of strings")
            return
        if len(values) == 0:
            if required or (lo or 0) > 0:
                self.fail(f"{name} needs at least one value")
            return
        self.check_items(name, values, lo=lo, hi=hi)
        parts = []
        for i, v in enumerate(values):
            s = str(v).strip()
            if s == "" or "," in s:
                self.fail(f"{name}[{i}] must be a non-empty value without commas")
            self._length(f"{name}[{i}]", s, item_lo, item_hi, False)
            parts.append(s)
        self.query.append((name, ",".join(parts)))

    def check_items(self, name: str, values: Sequence[Any] | None, *, lo: int | None = None, hi: int | None = None) -> None:
        if values is None or isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            self.fail(f"{name} must be a list")
            return
        if (lo is not None and len(values) < lo) or (hi is not None and len(values) > hi):
            self.fail(f"{name} must hold {_range(lo, hi)} items, got {len(values)}")


def _encode(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        out = {}
        for f in dataclasses.fields(value):
            v = getattr(value, f.name)
            if v is not None:
                out[f.metadata.get("json", f.name)] = _encode(v)
        return out
    if isinstance(value, Mapping):
        return {str(k): _encode(v) for k, v in value.items() if v is not None}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    return value


def encode_body(body: Mapping[str, Any]) -> dict[str, Any]:
    return {str(k): _encode(v) for k, v in body.items() if v is not None}


class _Page(Protocol[T_co]):
    @property
    def items(self) -> Sequence[T_co]: ...

    @property
    def next_cursor(self) -> str | None: ...


def _max_pages(options: RequestOptions | None) -> int | None:
    return options.max_pages if options is not None else None


class _CursorLoop:
    __slots__ = ("mark", "steps", "span")

    def __init__(self) -> None:
        self.mark: str | None = None
        self.steps = 0
        self.span = 0

    def repeats(self, current: str | None, nxt: str) -> bool:
        if nxt == current or nxt == self.mark:
            return True
        self.steps += 1
        if self.steps >= self.span:
            self.mark, self.steps, self.span = nxt, 0, max(1, 2 * self.span)
        return False


def _repeated(nxt: str) -> AveeValidationError:
    return AveeValidationError(f"the server repeated cursor {nxt[:64]!r}")


def paginate(fetch: Callable[[str | None], _Page[T]], start: str | None, options: RequestOptions | None) -> Iterator[T]:
    loops = _CursorLoop()
    cursor = start
    limit = _max_pages(options)
    pages = 0
    while limit is None or pages < limit:
        page = fetch(cursor)
        pages += 1
        yield from page.items
        nxt = page.next_cursor
        if not nxt:
            return
        if loops.repeats(cursor, nxt):
            raise _repeated(nxt)
        cursor = nxt


async def apaginate(fetch: Callable[[str | None], Awaitable[_Page[T]]], start: str | None, options: RequestOptions | None) -> AsyncIterator[T]:
    loops = _CursorLoop()
    cursor = start
    limit = _max_pages(options)
    pages = 0
    while limit is None or pages < limit:
        page = await fetch(cursor)
        pages += 1
        for item in page.items:
            yield item
        nxt = page.next_cursor
        if not nxt:
            return
        if loops.repeats(cursor, nxt):
            raise _repeated(nxt)
        cursor = nxt
