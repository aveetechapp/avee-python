from __future__ import annotations

import asyncio
import base64
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

import avee
from avee import (
    DEFAULT_BASE_URL,
    OPERATIONS,
    PREVIEW_BASE_URL,
    AsyncAveeClient,
    AveeAPIError,
    AveeClient,
    AveeConnectionError,
    AveeError,
    AveePaymentError,
    AveeTimeoutError,
    AveeValidationError,
    PaymentContext,
    PaymentSignature,
    RequestOptions,
    models,
    paginate,
)

COUNTS = {"buys": 1, "sells": 1, "buy_volume": 1.5, "sell_volume": 1.5, "buyers": 1, "sellers": 1}
WINDOWS = {"m5": 1.5, "h1": 1.5, "h6": 1.5, "h24": 1.5}
CHALLENGE = {
    "x402Version": 2,
    "accepts": [{"scheme": "exact", "network": "eip155:84532", "amount": "2000", "maxAmountRequired": "2000", "asset": "0xasset", "payTo": "0xpayee", "maxTimeoutSeconds": 60}],
}


def b64(v: Any) -> str:
    return base64.b64encode(json.dumps(v).encode()).decode()


def pair(address: str) -> dict[str, Any]:
    return {
        "network": {"slug": "base", "chain_id": 8453}, "dex": {"name": "uniswap", "factory": "0xf", "chain_id": 8453, "lp_token_symbol": "UNI-V2"},
        "pair_address": address, "ticker": "WETH/USDC", "pair_fee": 0.3, "tokens": [],
        "txn": {"m5": COUNTS, "h1": COUNTS, "h6": COUNTS, "h24": COUNTS}, "volume": WINDOWS, "price_change": WINDOWS,
        "liquidity_usd": 1000.0, "last_updated_at": "2026-09-28T00:00:00Z", "created_at": "2026-09-28T00:00:00Z",
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:
        pass

    def send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        for k, v in {"content-type": "application/json", "x-request-id": "req-1", **(headers or {})}.items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parts = urlsplit(self.path)
        query = parse_qs(parts.query)
        if parts.path == "/api/v1/chains":
            self.send(200, {"items": [{"slug": "base", "chain_id": 8453, "latest_block": 1, "block_lag_seconds": 0, "protocols": []}]})
        elif parts.path == "/api/v1/pairs":
            if "cursor" not in query:
                self.send(200, {"items": [pair("a1"), pair("a2")], "next_cursor": "p2"})
            else:
                self.send(200, {"items": [pair("a3")]})
        elif parts.path == "/api/v1/status":
            if "payment-signature" not in self.headers:
                self.send(402, CHALLENGE, {"payment-required": b64(CHALLENGE)})
            else:
                self.send(200, {"status": "ok"}, {"payment-response": b64({"success": True, "transaction": "0xtx", "network": "eip155:84532"})})
        else:
            body = {"type": "t", "title": "chain not found", "status": 404, "code": "chain_not_found", "detail": "no chain nope", "param": "chain", "request_id": "req-1"}
            self.send(404, body, {"content-type": "application/problem+json"})


@pytest.fixture
def base_url() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/api/v1"
    server.shutdown()
    server.server_close()
    thread.join(5)


class WalletPayer:
    networks = ["eip155:84532"]

    def sign(self, context: PaymentContext) -> PaymentSignature | None:
        if context.requirement.network != "eip155:84532" or not context.payment_id or context.operation != "status":
            return None
        return PaymentSignature(payload={"signature": "0xsig"})


class Declining:
    networks: list[str] = []

    def sign(self, context: PaymentContext) -> PaymentSignature | None:
        return None


def test_v0_1_0_client_operations_iterators_errors_and_payer(base_url: str) -> None:
    assert DEFAULT_BASE_URL.startswith("https://") and PREVIEW_BASE_URL.startswith("https://") and avee.__version__
    assert any(op[:3] == ("pairs", "GET", "/pairs") for op in OPERATIONS)

    with AveeClient(
        base_url, api_key="k1", timeout=5.0, max_retries=1, max_retry_delay=1.0, max_response_bytes=1 << 20, headers={"x-trace": "compat"}, payer=WalletPayer()
    ) as client:
        chains = client.chains()
        assert isinstance(chains, models.ChainList) and chains.items[0].slug == "base"
        info = client.last_response
        assert info is not None and (info.operation, info.status, info.request_id) == ("chains", 200, "req-1")

        page = client.pairs(chains=["base"], limit=2, min_liquidity_usd=1000)
        assert [p.pair_address for p in page.items] == ["a1", "a2"] and page.next_cursor == "p2"
        assert [p.pair_address for p in client.iter_pairs(options=RequestOptions(max_pages=5))] == ["a1", "a2", "a3"]

        with pytest.raises(AveeAPIError) as problem:
            client.pair("nope", "0xabc")
        e = problem.value
        assert isinstance(e, AveeError)
        assert (e.status, e.code, e.detail, e.param, e.request_id, e.retryable, e.paid) == (404, "chain_not_found", "no chain nope", "chain", "req-1", False, False)

        with pytest.raises(AveeValidationError):
            client.pairs(limit=1000)

        assert client.status().status == "ok"
        receipt = client.last_response.payment if client.last_response else None
        assert receipt is not None and receipt.success and receipt.transaction == "0xtx"

    with AveeClient(base_url, payer=Declining()) as declining, pytest.raises(AveePaymentError) as declined:
        declining.status()
    assert declined.value.operation == "status"

    with AveeClient("http://127.0.0.1:1/api/v1", max_retries=0) as dead, pytest.raises(AveeConnectionError):
        dead.chains()
    assert issubclass(AveeTimeoutError, AveeError)

    pages = {None: models.PairPage(items=[], next_cursor="b"), "b": models.PairPage(items=[], next_cursor=None)}
    assert list(paginate(lambda c: pages[c], None, None)) == []

    async def run() -> list[str]:
        async with AsyncAveeClient(base_url) as ac:
            assert (await ac.chains()).items[0].chain_id == 8453
            return [p.pair_address async for p in ac.iter_pairs()]

    assert asyncio.run(run()) == ["a1", "a2", "a3"]
