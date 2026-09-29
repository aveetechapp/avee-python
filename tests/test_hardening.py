from __future__ import annotations

import asyncio
import base64
import json
import socket
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

from avee import (
    AsyncAveeClient,
    AveeAPIError,
    AveeClient,
    AveePaymentError,
    AveeTimeoutError,
    AveeValidationError,
    PaymentContext,
    PaymentSignature,
    models,
    paginate,
)
from avee._request import _CursorLoop
from avee.x402 import offer_problem

from .conftest import SCHEMAS, Reply, instance


def b64(v: Any) -> str:
    return base64.b64encode(v if isinstance(v, bytes) else json.dumps(v).encode()).decode()


def offer(chain: str, **overrides: Any) -> dict[str, Any]:
    o = {
        "scheme": "exact", "network": chain, "amount": "2000", "maxAmountRequired": "2000",
        "asset": "0x036CbD53842c5426634e7929541eC2318f3dCF7e", "payTo": "0x00000000000000000000000000000000000000b2", "maxTimeoutSeconds": 60,
    }
    o.update(overrides)
    return {k: v for k, v in o.items() if v is not None}


def challenge(*accepts: Any) -> dict[str, Any]:
    return {"x402Version": 2, "resource": {"url": "https://api.preview.avee.tech/api/v1/chains"}, "accepts": list(accepts)}


def paywall(doc: Any) -> Reply:
    return Reply(402, doc, {"payment-required": b64(doc)})


def valid(schema: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = instance(SCHEMAS[schema], "required")
    body.update(overrides)
    return body


class Refusing:
    networks: list[str] = []

    def __init__(self) -> None:
        self.asked = 0

    def sign(self, context: PaymentContext) -> PaymentSignature | None:
        self.asked += 1
        return None


class Paying:
    networks: list[str] = []

    def __init__(self) -> None:
        self.asked = 0

    def sign(self, context: PaymentContext) -> PaymentSignature:
        self.asked += 1
        return PaymentSignature(payload={"signature": "0x"})


def test_redirects_are_never_followed(serve: Any) -> None:
    other = serve(lambda req, n: Reply(200, valid("StatusResponse")))

    def handler(req: Any, n: int) -> Reply:
        if req.path == "/api/v1/chains" and "payment-signature" not in req.headers:
            return paywall(challenge(offer("eip155:84532")))
        return Reply(307, {}, {"location": other.base_url + "/steal"})

    fake = serve(handler)
    with AveeClient(fake.base_url, api_key="k1", payer=Paying()) as c:
        with pytest.raises(AveeAPIError) as plain:
            c.status()
        assert (plain.value.status, plain.value.retryable) == (307, False)
        with pytest.raises(AveeAPIError) as paid:
            c.chains()
        assert (paid.value.status, paid.value.paid) == (307, True)
    assert len(other.requests) == 0 and len(fake.requests) == 3


@pytest.mark.parametrize(
    "reply",
    [
        Reply(402, challenge(offer("eip155:84532")), {"payment-required": "%%%"}),
        Reply(402, None, {"payment-required": b64(b"{not json")}),
        Reply(402, None, {"payment-required": b64(b"\xff\xfe{")}),
        paywall(challenge()),
        paywall({"x402Version": 2, "accepts": ["exact"]}),
        Reply(402, raw=b"pay up"),
    ],
    ids=["header not base64", "header not JSON", "header not UTF-8", "no accepts", "accepts of the wrong type", "body not JSON"],
)
def test_unreadable_challenges_are_refused_before_the_payer(serve: Any, reply: Reply) -> None:
    fake = serve(lambda req, n: reply)
    payer = Refusing()
    with AveeClient(fake.base_url, payer=payer) as c, pytest.raises(AveePaymentError, match="unreadable"):
        c.chains()
    assert (payer.asked, len(fake.requests)) == (0, 1)


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"}, {"amount": "-5"}, {"amount": "1.5"}, {"amount": ""}, {"amount": "9" * 79},
        {"amount": "2001", "maxAmountRequired": "2000"}, {"amount": 2000}, {"payTo": ""}, {"asset": ""}, {"network": 8453},
    ],
)
def test_malformed_offers_are_refused_before_the_payer(serve: Any, overrides: dict[str, Any]) -> None:
    fake = serve(lambda req, n: paywall(challenge(offer("eip155:84532", **overrides))))
    payer = Refusing()
    with AveeClient(fake.base_url, payer=payer) as c, pytest.raises(AveePaymentError):
        c.chains()
    assert (payer.asked, len(fake.requests)) == (0, 1)


@pytest.mark.parametrize(
    ("amount", "maximum", "ok"),
    [
        ("2000", "2000", True), ("1", "", True), ("0002000", "2000", True), ("9" * 78, "9" * 78, True),
        ("2001", "2000", False), ("10000", "9999", False), ("000", "", False), ("+1", "", False), (" 1", "", False),
        ("1_000", "", False), ("١", "", False), ("1", "0", False),
    ],
)
def test_offer_amount_rules(amount: str, maximum: str, ok: bool) -> None:
    o = models.PaymentRequirements._from_json(offer("eip155:1", amount=amount, maxAmountRequired=maximum))
    assert (offer_problem(o) is None) is ok


def test_a_paid_request_that_times_out_is_never_retried(serve: Any) -> None:
    def handler(req: Any, n: int) -> Reply:
        if "payment-signature" not in req.headers:
            return paywall(challenge(offer("eip155:84532")))
        return Reply(200, valid("ChainList"), delay=5)

    fake = serve(handler)
    payer = Paying()
    with AveeClient(fake.base_url, payer=payer, timeout=0.2, max_retry_delay=0.001) as c, pytest.raises(AveeTimeoutError):
        c.chains()
    assert (payer.asked, len(fake.requests)) == (1, 2)


def test_a_signature_of_the_wrong_shape_is_refused(serve: Any) -> None:
    fake = serve(lambda req, n: paywall(challenge(offer("eip155:84532"))))

    class Odd:
        networks: list[str] = []

        def __init__(self, result: Any) -> None:
            self.result = result

        def sign(self, context: PaymentContext) -> Any:
            return self.result

    for result in ("0xsig", PaymentSignature(header="a\r\nb"), PaymentSignature(), PaymentSignature(payload={"v": float("nan")})):
        with AveeClient(fake.base_url, payer=Odd(result)) as c, pytest.raises(AveePaymentError):
            c.chains()
    assert len(fake.requests) == 4


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"chains": []}, r"Config\.resolutions is missing"),
        ("config-with-string", r"Config\.min_liquidity_usd must be a number"),
        ([1, 2], r"Config must be an object"),
    ],
)
def test_responses_that_break_the_contract_are_errors(serve: Any, body: Any, message: str) -> None:
    if body == "config-with-string":
        body = valid("Config", min_liquidity_usd="7")
    fake = serve(lambda req, n: Reply(200, body))
    with AveeClient(fake.base_url) as c, pytest.raises(AveeValidationError, match=message):
        c.config()


@pytest.mark.parametrize("raw", [b"[" * 100_000, b"not json", b'{"status": "ok"', b"\xff\xfe"])
def test_unparseable_responses_are_errors(serve: Any, raw: bytes) -> None:
    fake = serve(lambda req, n: Reply(200, raw=raw))
    with AveeClient(fake.base_url) as c, pytest.raises(AveeValidationError):
        c.status()


@pytest.mark.parametrize("raw", [b'{"code":5,"title":["x"]}', b'{"code":', b"[]", b"null", "é".encode() * 300])
def test_malformed_problem_details_keep_the_body(serve: Any, raw: bytes) -> None:
    fake = serve(lambda req, n: Reply(404, raw=raw, headers={"content-type": "application/problem+json"}))
    with AveeClient(fake.base_url) as c, pytest.raises(AveeAPIError) as info:
        c.chains()
    e = info.value
    assert (e.status, e.code, e.problem) == (404, None, None)
    assert len(e.detail or "") <= 201


def test_a_long_problem_detail_is_clipped(serve: Any) -> None:
    fake = serve(lambda req, n: Reply(500, {"code": "internal", "title": "t", "detail": "x" * 5000}, {"content-type": "application/problem+json"}))
    with AveeClient(fake.base_url, max_retries=0) as c, pytest.raises(AveeAPIError) as info:
        c.chains()
    assert info.value.code == "internal" and len(info.value.detail or "") <= 201 and len(str(info.value)) < 400


def test_options_refuse_reserved_and_multi_line_headers_and_a_malformed_payer() -> None:
    for kwargs in ({"headers": {"Payment-Signature": "x"}}, {"headers": {"x-trace": "a\r\nx-injected: 1"}}, {"api_key": "a\x00b"}):
        with pytest.raises(AveeValidationError):
            AveeClient(**kwargs)

    class BadNetworks(Refusing):
        networks = "eip155:1"

    with pytest.raises(AveeValidationError):
        AveeClient(payer=BadNetworks())
    AveeClient(headers={"x-trace": "abc"}).close()


@pytest.fixture
def dripping() -> Iterator[str]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(0.05)
    stop = threading.Event()

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with conn:
                conn.recv(65536)
                try:
                    conn.sendall(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1000\r\n\r\n")
                    for _ in range(1000):
                        if stop.wait(0.05):
                            return
                        conn.sendall(b" ")
                except OSError:
                    continue

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{listener.getsockname()[1]}/api/v1"
    stop.set()
    listener.close()
    thread.join(5)


def test_a_dripping_response_hits_the_total_deadline(dripping: str) -> None:
    started = time.monotonic()
    with AveeClient(dripping, timeout=0.3, max_retries=0) as c, pytest.raises(AveeTimeoutError):
        c.status()
    assert time.monotonic() - started < 2


def test_async_dripping_response_hits_the_total_deadline_and_leaves_no_task(dripping: str) -> None:
    async def run() -> None:
        async with AsyncAveeClient(dripping, timeout=0.3, max_retries=0) as c:
            with pytest.raises(AveeTimeoutError):
                await c.status()
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(run())


def test_cursor_loops_of_any_length_stop_the_walk() -> None:
    cycle = ["c1", "c2", "c3", "c4", "c5"]
    fetched = 0

    def fetch(cursor: str | None) -> Any:
        nonlocal fetched
        fetched += 1
        nxt = cycle[(cycle.index(cursor) + 1) % len(cycle)] if cursor in cycle else cycle[0]
        return models.PairPage(items=[], next_cursor=nxt)

    with pytest.raises(AveeValidationError):
        list(paginate(fetch, None, None))
    assert fetched <= 3 * len(cycle) + 2


def test_the_cursor_loop_never_flags_distinct_cursors_and_holds_no_history() -> None:
    loops = _CursorLoop()
    prev: str | None = None
    for i in range(100_000):
        nxt = f"c{i}"
        assert not loops.repeats(prev, nxt)
        prev = nxt
    assert loops.repeats(prev, "c99999")


def test_the_iterator_fetches_nothing_once_the_consumer_stops() -> None:
    fetched = 0

    def fetch(cursor: str | None) -> Any:
        nonlocal fetched
        fetched += 1
        return models.PairPage(items=[], next_cursor=(cursor or "") + "x") if fetched > 1 else models.PairPage(items=["a", "b"], next_cursor="x")

    it = paginate(fetch, None, None)
    assert next(it) == "a"
    it.close()
    assert fetched == 1
