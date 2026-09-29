from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

from .errors import AstraValidationError
from .ids import normalize_feed_id
from .price import MAX_EXPO, MIN_EXPO, Price

Channel = Literal["real_time", "fixed_rate@200ms", "fixed_rate@1000ms"]
KNOWN_STATUSES = frozenset({"trading", "degraded", "market_closed", "reference", "no_data"})

_MAX_UNIX_SECONDS = 100_000_000_000
_MAX_UNIX_MS = 100_000_000_000_000
_MAX_COUNT = 1_000_000
_SIGNED = re.compile(r"-?[0-9]{1,80}")
_UNSIGNED = re.compile(r"[0-9]{1,80}")

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class UpdateMetadata:
    receive_time: int
    prev_publish_time: int


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    id: str
    price: Price
    ema_price: Price
    metadata: UpdateMetadata | None = None


@dataclass(frozen=True, slots=True)
class FeedMetadata:
    id: str
    astra_id: str
    symbol: str
    asset_type: str
    display_symbol: str
    quote_currency: str
    description: str
    min_channel: str
    market_open: bool
    attributes: Mapping[str, str]
    base: str | None = None
    schedule: str | None = None


@dataclass(frozen=True, slots=True)
class LiveValue:
    status: str
    expo: int
    publish_time: int
    timestamp_ms: int
    served_publish_time: int
    sources: int
    price: Price | None = None


@dataclass(frozen=True, slots=True)
class Feed:
    id: str
    symbol: str
    category: str
    live: LiveValue
    attributes: Mapping[str, str] = field(default_factory=dict)
    pyth_id: str | None = None


@dataclass(frozen=True, slots=True)
class FeedIdEntry:
    symbol: str
    asset_type: str
    category: str
    astra_id: str
    pyth_id: str | None = None


@dataclass(frozen=True, slots=True)
class FeedIdMap:
    items: list[FeedIdEntry]
    missing: list[str] | None = None


@dataclass(frozen=True, slots=True)
class FeedHealth:
    id: str
    symbol: str
    status: str
    age_seconds: float
    publish_time: int
    served_publish_time: int
    sources: int
    stale: bool


@dataclass(frozen=True, slots=True)
class StatusReport:
    ready: bool
    feeds: list[FeedHealth]


@dataclass(frozen=True, slots=True)
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float


def _invalid(path: str, what: str) -> AstraValidationError:
    return AstraValidationError(f"malformed Astra response at {path}: {what}")


def _obj(v: Any, path: str) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise _invalid(path, "expected an object")
    return v


def _list(v: Any, path: str) -> list[Any]:
    if not isinstance(v, list):
        raise _invalid(path, "expected an array")
    return v


def _str(o: dict[str, Any], key: str, path: str) -> str:
    v = o.get(key)
    if not isinstance(v, str):
        raise _invalid(f"{path}.{key}", "expected a string")
    return v


def _opt_str(o: dict[str, Any], key: str, path: str) -> str | None:
    v = o.get(key)
    if v is None:
        return None
    if not isinstance(v, str):
        raise _invalid(f"{path}.{key}", "expected a string")
    return v


def _int(o: dict[str, Any], key: str, path: str, lo: int, hi: int) -> int:
    v = o.get(key)
    if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
        raise _invalid(f"{path}.{key}", f"expected an integer in [{lo}, {hi}]")
    return v


def _number(v: Any, path: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise _invalid(path, "expected a finite number")
    return float(v)


def _bool(o: dict[str, Any], key: str, path: str) -> bool:
    v = o.get(key)
    if not isinstance(v, bool):
        raise _invalid(f"{path}.{key}", "expected a boolean")
    return v


def _feed_id(o: dict[str, Any], key: str, path: str) -> str:
    try:
        return normalize_feed_id(_str(o, key, path))
    except AstraValidationError:
        raise _invalid(f"{path}.{key}", "expected a 32-byte hex feed id") from None


def _mantissa(o: dict[str, Any], key: str, path: str, pattern: re.Pattern[str]) -> str:
    v = _str(o, key, path)
    if not pattern.fullmatch(v):
        raise _invalid(f"{path}.{key}", "expected an integer string")
    return v


def _string_attributes(v: Any, path: str) -> dict[str, str]:
    return {k: val for k, val in _obj(v, path).items() if isinstance(val, str)}


def decode_price(v: Any, path: str) -> Price:
    o = _obj(v, path)
    return Price(
        price=_mantissa(o, "price", path, _SIGNED),
        conf=_mantissa(o, "conf", path, _UNSIGNED),
        expo=_int(o, "expo", path, MIN_EXPO, MAX_EXPO),
        publish_time=_int(o, "publish_time", path, 0, _MAX_UNIX_SECONDS),
    )


def _metadata(v: Any, path: str, receive_key: str) -> UpdateMetadata | None:
    if v is None:
        return None
    o = _obj(v, path)
    if o.get(receive_key) is None or o.get("prev_publish_time") is None:
        return None
    return UpdateMetadata(
        receive_time=_int(o, receive_key, path, 0, _MAX_UNIX_SECONDS),
        prev_publish_time=_int(o, "prev_publish_time", path, 0, _MAX_UNIX_SECONDS),
    )


def _update(v: Any, path: str, receive_key: str) -> PriceUpdate:
    o = _obj(v, path)
    return PriceUpdate(
        id=_feed_id(o, "id", path),
        price=decode_price(o.get("price"), f"{path}.price"),
        ema_price=decode_price(o.get("ema_price"), f"{path}.ema_price"),
        metadata=_metadata(o.get("metadata"), f"{path}.metadata", receive_key),
    )


def decode_parsed_update(v: Any, path: str) -> PriceUpdate:
    return _update(v, path, "proof_available_time")


def decode_streamed_price_feed(v: Any, path: str) -> PriceUpdate:
    return _update(v, path, "price_service_receive_time")


def decode_update_envelope(v: Any, path: str) -> list[PriceUpdate]:
    o = _obj(v, path)
    parsed = o.get("parsed")
    if parsed is None:
        return []
    return [decode_parsed_update(p, f"{path}.parsed[{i}]") for i, p in enumerate(_list(parsed, f"{path}.parsed"))]


def decode_feed_metadata(v: Any, path: str) -> FeedMetadata:
    o = _obj(v, path)
    ap = f"{path}.attributes"
    a = _obj(o.get("attributes"), ap)
    hours = _obj(o.get("market_hours"), f"{path}.market_hours")
    return FeedMetadata(
        id=_feed_id(o, "id", path),
        astra_id=_feed_id(a, "astra_id", ap),
        symbol=_str(a, "symbol", ap),
        asset_type=_str(a, "asset_type", ap),
        display_symbol=_str(a, "display_symbol", ap),
        quote_currency=_str(a, "quote_currency", ap),
        description=_str(a, "description", ap),
        min_channel=_str(a, "min_channel", ap),
        market_open=_bool(hours, "is_open", f"{path}.market_hours"),
        attributes=_string_attributes(a, ap),
        base=_opt_str(a, "base", ap),
        schedule=_opt_str(a, "schedule", ap),
    )


def _live(v: Any, path: str) -> LiveValue:
    o = _obj(v, path)
    expo = _int(o, "expo", path, MIN_EXPO, MAX_EXPO)
    publish_time = _int(o, "publish_time", path, 0, _MAX_UNIX_SECONDS)
    price = None
    if o.get("price") is not None:
        price = Price(
            price=_mantissa(o, "price", path, _SIGNED),
            conf=_mantissa(o, "conf", path, _UNSIGNED),
            expo=expo,
            publish_time=publish_time,
        )
    return LiveValue(
        status=_str(o, "status", path),
        expo=expo,
        publish_time=publish_time,
        timestamp_ms=_int(o, "timestamp_ms", path, 0, _MAX_UNIX_MS),
        served_publish_time=_int(o, "served_publish_time", path, 0, _MAX_UNIX_SECONDS),
        sources=_int(o, "sources", path, 0, _MAX_COUNT),
        price=price,
    )


def decode_feed(v: Any, path: str) -> Feed:
    o = _obj(v, path)
    pyth = o.get("pyth_id")
    return Feed(
        id=_feed_id(o, "id", path),
        symbol=_str(o, "symbol", path),
        category=_str(o, "category", path),
        live=_live(o.get("live"), f"{path}.live"),
        attributes={} if o.get("attributes") is None else _string_attributes(o["attributes"], f"{path}.attributes"),
        pyth_id=None if pyth is None else _feed_id(o, "pyth_id", path),
    )


def decode_feed_id_entry(v: Any, path: str) -> FeedIdEntry:
    o = _obj(v, path)
    return FeedIdEntry(
        symbol=_str(o, "symbol", path),
        asset_type=_str(o, "asset_type", path),
        category=_str(o, "category", path),
        astra_id=_feed_id(o, "astra_id", path),
        pyth_id=None if o.get("pyth_id") is None else _feed_id(o, "pyth_id", path),
    )


def decode_feed_id_map(v: Any, path: str) -> FeedIdMap:
    o = _obj(v, path)
    items = [decode_feed_id_entry(e, f"{path}.items[{i}]") for i, e in enumerate(_list(o.get("items"), f"{path}.items"))]
    if o.get("missing") is None:
        return FeedIdMap(items=items)
    missing = _list(o["missing"], f"{path}.missing")
    return FeedIdMap(items=items, missing=[_feed_id({"id": m}, "id", f"{path}.missing[{i}]") for i, m in enumerate(missing)])


def _feed_health(v: Any, path: str) -> FeedHealth:
    o = _obj(v, path)
    return FeedHealth(
        id=_feed_id(o, "id", path),
        symbol=_str(o, "symbol", path),
        status=_str(o, "status", path),
        age_seconds=_number(o.get("age_seconds"), f"{path}.age_seconds"),
        publish_time=_int(o, "publish_time", path, 0, _MAX_UNIX_SECONDS),
        served_publish_time=_int(o, "served_publish_time", path, 0, _MAX_UNIX_SECONDS),
        sources=_int(o, "sources", path, 0, _MAX_COUNT),
        stale=_bool(o, "stale", path),
    )


def decode_status_report(v: Any, path: str) -> StatusReport:
    o = _obj(v, path)
    feeds = [_feed_health(f, f"{path}.feeds[{i}]") for i, f in enumerate(_list(o.get("feeds"), f"{path}.feeds"))]
    return StatusReport(ready=_bool(o, "ready", path), feeds=feeds)


def decode_candles(v: Any, path: str) -> list[Candle]:
    o = _obj(v, path)
    status = _str(o, "s", path)
    if status == "no_data":
        return []
    if status != "ok":
        raise _invalid(f"{path}.s", f"unexpected status {status[:40]!r}")
    t = _list(o.get("t"), f"{path}.t")
    cols = [_list(o.get(k), f"{path}.{k}") for k in ("o", "h", "l", "c")]
    if any(len(c) != len(t) for c in cols):
        raise _invalid(path, "bar arrays differ in length")
    out = []
    for i, time in enumerate(t):
        if isinstance(time, bool) or not isinstance(time, int) or not 0 <= time <= _MAX_UNIX_SECONDS:
            raise _invalid(f"{path}.t[{i}]", "expected unix seconds")
        o_, h, low, c = (_number(col[i], f"{path}.{k}[{i}]") for col, k in zip(cols, "ohlc"))
        out.append(Candle(time=time, open=o_, high=h, low=low, close=c))
    return out


def decode_list(v: Any, path: str, item: Callable[[Any, str], T]) -> list[T]:
    return [item(x, f"{path}[{i}]") for i, x in enumerate(_list(v, path))]
