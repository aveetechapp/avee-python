from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest

from avee import (
    DEFAULT_BASE_URL,
    AsyncAveeClient,
    AveeAPIError,
    AveeClient,
    AveeConnectionError,
    AveeTimeoutError,
    AveeValidationError,
    RequestOptions,
)

from .conftest import SCHEMAS, Reply, instance, problem


def test_validation_happens_before_any_request(serve: Any) -> None:
    fake = serve(lambda req, n: Reply(200, {}))
    c = AveeClient(fake.base_url)
    bad: list[Callable[[], Any]] = [
        lambda: c.pair("", "0xabc"),
        lambda: c.pair("base", ".."),
        lambda: c.pair(-1, "0xabc"),
        lambda: c.pairs(limit=101),
        lambda: c.pairs(chains=["base"] * 33),
        lambda: c.pairs(chains=["a,b"]),
        lambda: c.pairs(chains="base"),
        lambda: c.search(q=""),
        lambda: c.token_by_id("nope"),
        lambda: c.pair_batch(items=[]),
        lambda: c.pair_batch(items=[{"chain": "base", "address": "0xa"}] * 51),
        lambda: c.token_batch(items=[{"chain": "base", "address": "0xa"}] * 201),
        lambda: c.wallets(chain="base", min_win_rate=1.5),
        lambda: c.pairs(min_liquidity_usd=float("nan")),
    ]
    for run in bad:
        with pytest.raises(AveeValidationError):
            run()
    assert fake.requests == []
    c.close()


def test_chain_ids_and_escaped_paths(serve: Any) -> None:
    fake = serve(lambda req, n: Reply(200, instance(SCHEMAS["WalletProfile"], "required")))
    with AveeClient(fake.base_url) as c:
        c.wallet_profile(950000, "EQ/A B?")
    assert fake.requests[0].path == "/api/v1/chains/950000/wallets/EQ%2FA%20B%3F"


def test_options_are_checked() -> None:
    for kwargs in ({"base_url": "ftp://x"}, {"base_url": "https://x/?a=1"}, {"api_key": "a\nb"}, {"timeout": 0}, {"max_retries": -1}):
        with pytest.raises(AveeValidationError):
            AveeClient(**kwargs)
    assert DEFAULT_BASE_URL.endswith("/api/v1")


def pages(req: Any, n: int) -> Reply:
    cursor = dict(req.query).get("cursor")
    if cursor is None:
        return Reply(200, {"items": [{"pair_address": "a1"}, {"pair_address": "a2"}], "next_cursor": "p2"})
    if cursor == "p2":
        return Reply(200, {"items": [{"pair_address": "a3"}], "next_cursor": "p3"})
    return Reply(200, {"items": [{"pair_address": "a4"}]})


def test_iterators_fetch_lazily(serve: Any, monkeypatch: Any) -> None:
    import avee.models as m

    real = m.PairInfo._from_json
    monkeypatch.setattr(m.PairInfo, "_from_json", classmethod(lambda cls, v, where="PairInfo": v["pair_address"]))
    fake = serve(pages)
    with AveeClient(fake.base_url) as c:
        assert list(c.iter_pairs(chains=["base"], limit=2)) == ["a1", "a2", "a3", "a4"]
        assert len(fake.requests) == 3
        assert ("cursor", "p2") in fake.requests[1].query and ("chains", "base") in fake.requests[1].query
        next(iter(c.iter_pairs()))
        assert len(fake.requests) == 4
        assert list(c.iter_pairs(options=RequestOptions(max_pages=2))) == ["a1", "a2", "a3"]

        async def walk() -> list[Any]:
            async with AsyncAveeClient(fake.base_url) as ac:
                return [p async for p in ac.iter_pairs()]

        assert asyncio.run(walk()) == ["a1", "a2", "a3", "a4"]
    monkeypatch.setattr(m.PairInfo, "_from_json", real)


def test_repeated_cursor_stops_the_walk(serve: Any, monkeypatch: Any) -> None:
    import avee.models as m

    monkeypatch.setattr(m.PairInfo, "_from_json", classmethod(lambda cls, v, where="PairInfo": v))
    fake = serve(lambda req, n: Reply(200, {"items": [{"pair_address": "a"}], "next_cursor": "same"}))
    seen = []
    with AveeClient(fake.base_url) as c, pytest.raises(AveeValidationError):
        for p in c.iter_trending():
            seen.append(p)
    assert len(seen) == 2


def test_problem_details_become_typed_errors(serve: Any) -> None:
    body = {"type": "t", "title": "chain not found", "status": 404, "code": "chain_not_found", "detail": "no chain nope", "param": "chain", "request_id": "r-9"}
    fake = serve(lambda req, n: Reply(404, body, {"content-type": "application/problem+json"}))
    with AveeClient(fake.base_url) as c, pytest.raises(AveeAPIError) as info:
        c.pair("nope", "0xabc")
    e = info.value
    assert (e.status, e.code, e.detail, e.param, e.request_id, e.retryable) == (404, "chain_not_found", "no chain nope", "chain", "r-9", False)
    assert len(fake.requests) == 1


def flaky(req: Any, n: int) -> Reply:
    if n == 1:
        return problem(429, "rate_limited", "slow down", {"retry-after": "1"})
    if n == 2:
        return problem(503, "upstream_unavailable", "later")
    return Reply(200, {"status": "ok"})


def test_retries_honour_retry_after(serve: Any) -> None:
    fake = serve(flaky)
    started = time.monotonic()
    with AveeClient(fake.base_url) as c:
        assert c.status().status == "ok"
    assert time.monotonic() - started >= 0.95
    assert len(fake.requests) == 3


def test_async_retries_honour_retry_after(serve: Any) -> None:
    fake = serve(flaky)

    async def run() -> str:
        async with AsyncAveeClient(fake.base_url) as c:
            return (await c.status()).status

    assert asyncio.run(run()) == "ok"
    assert len(fake.requests) == 3


def test_post_is_never_retried(serve: Any) -> None:
    fake = serve(lambda req, n: problem(503, "upstream_unavailable", "later"))
    with AveeClient(fake.base_url) as c, pytest.raises(AveeAPIError):
        c.pair_batch(items=[{"chain": "base", "address": "0xa"}])
    assert len(fake.requests) == 1


def test_retry_after_beyond_the_cap_is_raised(serve: Any) -> None:
    fake = serve(lambda req, n: problem(429, "rate_limited", "later", {"retry-after": "120"}))
    with AveeClient(fake.base_url, max_retry_delay=1) as c, pytest.raises(AveeAPIError) as info:
        c.chains()
    assert info.value.retry_after == 120 and len(fake.requests) == 1


def test_timeouts_are_retried_then_raised(serve: Any) -> None:
    fake = serve(lambda req, n: Reply(200, {"status": "ok"}, delay=5))
    with AveeClient(fake.base_url, timeout=0.1, max_retries=1, max_retry_delay=0.01) as c, pytest.raises(AveeTimeoutError):
        c.status()
    assert len(fake.requests) == 2


def test_connection_failures() -> None:
    with AveeClient("http://127.0.0.1:1/api/v1", max_retries=0) as c, pytest.raises(AveeConnectionError):
        c.status()


def test_oversized_responses_are_refused(serve: Any) -> None:
    fake = serve(lambda req, n: Reply(200, {"status": "x" * 4096}))
    with AveeClient(fake.base_url, max_response_bytes=1024) as c, pytest.raises(AveeValidationError):
        c.status()


def test_rate_limit_of_the_last_response_and_the_key(serve: Any) -> None:
    headers = {"ratelimit-policy": '"plan";q=5;w=1;burst=20', "ratelimit": '"plan";r=19;t=1', "x-ratelimit-limit": "5", "x-request-id": "abc"}
    fake = serve(lambda req, n: Reply(200, {"status": "ok"}, headers))
    with AveeClient(fake.base_url, api_key="k1") as c:
        c.status()
        info = c.last_response
    assert info is not None
    assert (info.operation, info.status, info.request_id) == ("status", 200, "abc")
    assert (info.rate_limit.limit, info.rate_limit.remaining, info.rate_limit.reset_seconds) == (5, 19, 1)
    assert "q=5" in (info.rate_limit.policy or "")
    assert fake.requests[0].headers["x-api-key"] == "k1"
    assert fake.requests[0].headers["user-agent"].startswith("avee-python/")
