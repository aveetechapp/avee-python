from __future__ import annotations

from typing import Any

import pytest

from avee.astra import AstraValidationError
from avee.astra.models import (
    decode_candles,
    decode_feed,
    decode_feed_metadata,
    decode_status_report,
    decode_streamed_price_feed,
    decode_update_envelope,
)

from .conftest import BTC, BTC_ASTRA, envelope, parsed, price


def metadata_body() -> dict[str, Any]:
    return {
        "id": BTC,
        "attributes": {
            "asset_type": "Crypto",
            "base": "BTC",
            "description": "d",
            "display_symbol": "BTC/USD",
            "quote_currency": "USD",
            "symbol": "Crypto.BTC/USD",
            "min_channel": "real_time",
            "astra_id": BTC_ASTRA,
            "schedule": "America/New_York;O,O,O,O,O,O,O;",
        },
        "market_hours": {"is_open": True, "next_open": None, "next_close": None},
    }


def test_decodes_an_envelope() -> None:
    [u] = decode_update_envelope(envelope(parsed("0x" + BTC.upper(), "6512345000000", 1790540000)), "u")
    assert u.id == BTC
    assert str(u.price.to_decimal().normalize()) == "65123.45"
    assert u.metadata is not None and u.metadata.prev_publish_time == 1790539999


def test_missing_parsed_means_no_updates() -> None:
    assert decode_update_envelope({"binary": {"encoding": "hex", "data": []}}, "u") == []


@pytest.mark.parametrize(
    "patch",
    [
        {"price": "12.5"},
        {"price": 125},
        {"price": ""},
        {"conf": "-1"},
        {"expo": -400},
        {"expo": -8.5},
        {"expo": True},
        {"publish_time": 1790540000000},
        {"publish_time": -1},
        {"publish_time": None},
    ],
)
def test_rejects_malformed_prices(patch: dict[str, Any]) -> None:
    item = parsed(BTC, "1", 1)
    item["price"] = {**price("1", 1), **patch}
    with pytest.raises(AstraValidationError):
        decode_update_envelope(envelope(item), "u")


def test_rejects_bad_id_missing_ema_and_wrong_shapes() -> None:
    with pytest.raises(AstraValidationError, match="id"):
        decode_update_envelope(envelope({**parsed(BTC, "1", 1), "id": "xyz"}), "u")
    item = parsed(BTC, "1", 1)
    del item["ema_price"]
    with pytest.raises(AstraValidationError, match="ema_price"):
        decode_update_envelope(envelope(item), "u")
    with pytest.raises(AstraValidationError, match="array"):
        decode_update_envelope({"parsed": {}}, "u")
    with pytest.raises(AstraValidationError, match="object"):
        decode_update_envelope(None, "u")


def test_tolerates_unknown_fields_and_incomplete_metadata() -> None:
    item = {**parsed(BTC, "1", 1), "future": {"nested": [1]}, "metadata": {"slot": 0}}
    [u] = decode_update_envelope({**envelope(item), "version": 2}, "u")
    assert u.metadata is None


def test_decodes_a_streamed_price_feed() -> None:
    u = decode_streamed_price_feed(
        {"id": BTC, "price": price("5", 10), "ema_price": price("6", 10), "metadata": {"price_service_receive_time": 11, "prev_publish_time": 9}},
        "f",
    )
    assert u.metadata is not None and u.metadata.receive_time == 11


def test_feed_metadata_keeps_only_string_attributes() -> None:
    body = metadata_body()
    body["attributes"] |= {"weight": 3, "future": "x"}
    m = decode_feed_metadata(body, "m")
    assert (m.astra_id, m.base, m.market_open) == (BTC_ASTRA, "BTC", True)
    assert m.attributes["future"] == "x" and "weight" not in m.attributes


def test_feed_metadata_optional_and_required_attributes() -> None:
    body = metadata_body()
    del body["attributes"]["base"], body["attributes"]["schedule"]
    m = decode_feed_metadata(body, "m")
    assert m.base is None and m.schedule is None
    del body["attributes"]["symbol"]
    with pytest.raises(AstraValidationError, match="symbol"):
        decode_feed_metadata(body, "m")


def test_native_feed_with_and_without_price() -> None:
    live = {"status": "trading", "price": "149606601500", "conf": "28398500", "expo": -9, "publish_time": 10,
            "timestamp_ms": 10500, "served_publish_time": 10, "sources": 6}
    f = decode_feed({"id": BTC_ASTRA, "pyth_id": BTC, "symbol": "S", "category": "crypto", "attributes": {}, "live": live}, "f")
    assert f.live.price is not None and f.live.price.to_float() == 149.6066015 and f.pyth_id == BTC
    no_data = {k: v for k, v in live.items() if k not in ("price", "conf")} | {"status": "halted_by_regulator"}
    f = decode_feed({"id": BTC_ASTRA, "symbol": "S", "category": "new_class", "live": no_data}, "f")
    assert f.live.price is None and f.live.status == "halted_by_regulator" and f.pyth_id is None
    with pytest.raises(AstraValidationError, match="conf"):
        decode_feed({"id": BTC_ASTRA, "symbol": "S", "category": "c", "live": {**no_data, "price": "1"}}, "f")


def test_status_report() -> None:
    feed = {"id": BTC_ASTRA, "symbol": "S", "status": "trading", "age_seconds": 0.3, "publish_time": 1,
            "served_publish_time": 1, "sources": 5, "stale": False}
    r = decode_status_report({"ready": True, "feeds": [feed]}, "s")
    assert r.ready and r.feeds[0].age_seconds == 0.3
    with pytest.raises(AstraValidationError):
        decode_status_report({"ready": True, "feeds": [{**feed, "stale": None}]}, "s")


def test_candles() -> None:
    bars = decode_candles({"s": "ok", "t": [60, 120], "o": [1, 2], "h": [3, 4], "l": [0.5, 1], "c": [2, 3], "v": [0, 0]}, "c")
    assert [(b.time, b.close) for b in bars] == [(60, 2.0), (120, 3.0)]
    assert decode_candles({"s": "no_data"}, "c") == []
    for bad in (
        {"s": "ok", "t": [60], "o": [], "h": [1], "l": [1], "c": [1]},
        {"s": "ok", "t": [60.5], "o": [1], "h": [1], "l": [1], "c": [1]},
        {"s": "ok", "t": [60], "o": ["1"], "h": [1], "l": [1], "c": [1]},
        {"s": "error", "errmsg": "x"},
    ):
        with pytest.raises(AstraValidationError):
            decode_candles(bad, "c")
