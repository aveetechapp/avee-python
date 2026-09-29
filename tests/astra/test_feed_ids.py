from __future__ import annotations

import json
from collections.abc import Awaitable, Callable

import pytest
from aiohttp import web

from avee.astra import FEED_IDS_PER_REQUEST, AstraClient, AstraHTTPError, AstraValidationError, AsyncAstraClient, FeedIdEntry

from .conftest import BTC, BTC_ASTRA, ETH, FakeAstra

TSLA_ASTRA = "22" * 32
BTC_ENTRY = {"symbol": "Crypto.BTC/USD", "asset_type": "Crypto", "category": "crypto", "astra_id": BTC_ASTRA, "pyth_id": BTC}
TSLA_ENTRY = {"symbol": "Equity.RH.TSLA/USD", "asset_type": "Equity", "category": "equity", "astra_id": TSLA_ASTRA}


def reply(body: object, status: int = 200) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.json_response(body, status=status)

    return handler


def test_reads_the_whole_map(fake: FakeAstra) -> None:
    fake.http = reply({"items": [BTC_ENTRY, TSLA_ENTRY]})
    with AstraClient(fake.url) as c:
        got = c.feed_ids()
    assert got.items == [
        FeedIdEntry(symbol="Crypto.BTC/USD", asset_type="Crypto", category="crypto", astra_id=BTC_ASTRA, pyth_id=BTC),
        FeedIdEntry(symbol="Equity.RH.TSLA/USD", asset_type="Equity", category="equity", astra_id=TSLA_ASTRA),
    ]
    assert got.missing is None
    assert fake.requests[0].path == "/v1/feed-ids" and not fake.requests[0].query


def test_sends_normalised_ids_in_one_request(fake: FakeAstra) -> None:
    unknown = "11" * 32
    fake.http = reply({"items": [BTC_ENTRY], "missing": [unknown]})
    with AstraClient(fake.url) as c:
        got = c.feed_ids(["0x" + BTC.upper(), BTC, unknown], [ETH], category="crypto")
    assert [e.pyth_id for e in got.items] == [BTC] and got.missing == [unknown]
    assert len(fake.requests) == 1
    q = fake.requests[0].query
    assert q["pyth_ids"] == f"{BTC},{unknown}" and q["astra_ids"] == ETH and q["category"] == "crypto"


def test_splits_a_long_list_and_merges_sorted(fake: FakeAstra) -> None:
    ids = [format(i, "064x") for i in range(FEED_IDS_PER_REQUEST + 1)]

    async def handler(request: web.Request) -> web.StreamResponse:
        asked = request.query["pyth_ids"].split(",")
        if len(asked) > 1:
            return web.json_response({"items": [TSLA_ENTRY, BTC_ENTRY], "missing": asked[1:]})
        return web.json_response({"items": [BTC_ENTRY], "missing": asked})

    fake.http = handler
    with AstraClient(fake.url) as c:
        got = c.feed_ids(pyth_ids=ids)
    assert len(fake.requests) == 2
    assert len(fake.requests[0].query["pyth_ids"].split(",")) == FEED_IDS_PER_REQUEST
    assert [e.symbol for e in got.items] == ["Crypto.BTC/USD", "Equity.RH.TSLA/USD"]
    assert got.missing == ids[1:]


def test_validates_before_any_request(fake: FakeAstra) -> None:
    fake.http = reply({"items": []})
    with AstraClient(fake.url) as c:
        empty = c.feed_ids(pyth_ids=[])
        with pytest.raises(AstraValidationError):
            c.feed_ids(astra_ids=["nope"])
        with pytest.raises(AstraValidationError):
            c.feed_ids(pyth_ids=BTC)
    assert empty.items == [] and empty.missing == []
    assert fake.requests == []


def test_problem_is_not_retried(fake: FakeAstra) -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        body = {"type": "about:blank", "title": "Bad Request", "status": 400, "detail": "Too many feed ids, the limit is 500"}
        return web.Response(status=400, text=json.dumps(body), content_type="application/problem+json")

    fake.http = handler
    with AstraClient(fake.url) as c, pytest.raises(AstraHTTPError) as err:
        c.feed_ids([BTC])
    assert err.value.status == 400 and len(fake.requests) == 1


@pytest.mark.parametrize(
    "body",
    [{}, {"items": [{"symbol": "x", "asset_type": "Crypto", "category": "crypto"}]}, {"items": [], "missing": ["zz"]}],
)
def test_rejects_a_malformed_map(fake: FakeAstra, body: object) -> None:
    fake.http = reply(body)
    with AstraClient(fake.url) as c, pytest.raises(AstraValidationError):
        c.feed_ids()


@pytest.mark.anyio
async def test_async_feed_ids_mirrors_the_sync_one(fake: FakeAstra) -> None:
    fake.http = reply({"items": [BTC_ENTRY], "missing": []})
    async with AsyncAstraClient(fake.url) as c:
        got = await c.feed_ids([BTC])
    assert got.items[0].astra_id == BTC_ASTRA and got.missing == []


def test_packs_both_lists_into_one_url_budget_and_merges_once(fake: FakeAstra) -> None:
    gone = "33" * 32
    pyth = [f"{i + 1000:064x}" for i in range(150)]
    astra = [f"{i + 2000:064x}" for i in range(100)]

    async def handler(request: web.Request) -> web.StreamResponse:
        if "pyth_ids" in request.query:
            return web.json_response({"items": [TSLA_ENTRY, BTC_ENTRY], "missing": [gone]})
        return web.json_response({"items": [BTC_ENTRY], "missing": [gone]})

    fake.http = handler
    with AstraClient(fake.url) as c:
        got = c.feed_ids(pyth, astra)
    first, second = (r.query for r in fake.requests)
    assert len(first["pyth_ids"].split(",")) == 150 and len(first["astra_ids"].split(",")) == 50
    assert "pyth_ids" not in second and second["astra_ids"] == ",".join(astra[50:])
    assert [e.symbol for e in got.items] == ["Crypto.BTC/USD", "Equity.RH.TSLA/USD"]
    assert got.missing == [gone]


def test_a_filtered_answer_without_missing_gives_an_empty_list(fake: FakeAstra) -> None:
    fake.http = reply({"items": [BTC_ENTRY]})
    with AstraClient(fake.url) as c:
        assert c.feed_ids([BTC]).missing == []


def test_fails_whole_when_a_later_chunk_fails(fake: FakeAstra) -> None:
    ids = [f"{i:064x}" for i in range(FEED_IDS_PER_REQUEST + 1)]

    async def handler(_: web.Request) -> web.StreamResponse:
        if len(fake.requests) > 1:
            return web.Response(status=404, text="gone")
        return web.json_response({"items": [BTC_ENTRY], "missing": []})

    fake.http = handler
    with AstraClient(fake.url) as c, pytest.raises(AstraHTTPError):
        c.feed_ids(ids)
    assert len(fake.requests) == 2


def test_rejects_a_single_string(fake: FakeAstra) -> None:
    with AstraClient(fake.url) as c, pytest.raises(AstraValidationError, match="single string"):
        c.feed_ids(BTC)
    assert fake.requests == []
