from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

import pytest

from avee import AsyncAveeClient, AveeAPIError, AveeClient, AveePaymentError, PaymentContext, PaymentSignature

from .conftest import SCHEMAS, Reply, instance, problem


def b64(v: Any) -> str:
    return base64.b64encode(json.dumps(v).encode()).decode()


def offer(network: str) -> dict[str, Any]:
    return {
        "scheme": "exact", "network": network, "amount": "2000", "maxAmountRequired": "2000",
        "asset": "0x036CbD53842c5426634e7929541eC2318f3dCF7e", "payTo": "0x00000000000000000000000000000000000000b2",
        "maxTimeoutSeconds": 60, "extra": {"name": "USDC", "version": "2", "x_future": 1},
    }


def challenge(*accepts: Any) -> dict[str, Any]:
    return {"x402Version": 2, "error": "keyless budget spent", "resource": {"url": "https://api.preview.avee.tech/api/v1/chains"}, "accepts": list(accepts)}


def paywall(doc: dict[str, Any], header: bool = True) -> Reply:
    headers = {"retry-after": "30", **({"payment-required": b64(doc)} if header else {})}
    return Reply(402, doc, headers)


class Recorder:
    def __init__(self, networks: list[str], result: PaymentSignature | None) -> None:
        self.networks = networks
        self.result = result
        self.seen: list[PaymentContext] = []

    def sign(self, context: PaymentContext) -> PaymentSignature | None:
        self.seen.append(context)
        return self.result


def paid_once(req: Any, n: int) -> Reply:
    if "payment-signature" not in req.headers:
        return paywall(challenge(offer("eip155:8453"), offer("eip155:84532")))
    receipt = b64({"success": True, "transaction": "0xtx", "network": "eip155:84532", "payer": "0xp"})
    return Reply(200, {"items": []}, {"payment-response": receipt})


def test_pays_once_on_the_preferred_network(serve: Any) -> None:
    fake = serve(paid_once)
    payer = Recorder(["eip155:84532", "eip155:8453"], PaymentSignature(payload={"signature": "0xsig", "authorization": {"from": "0xp"}}))
    with AveeClient(fake.base_url, payer=payer) as c:
        c.chains()
        receipt = c.last_response.payment if c.last_response else None
    assert len(fake.requests) == 2 and len(payer.seen) == 1
    assert fake.requests[0].headers["accept-payment"] == "x402"
    ctx = payer.seen[0]
    assert (ctx.requirement.network, ctx.requirement.amount, ctx.requirement.pay_to, ctx.operation) == (
        "eip155:84532", "2000", "0x00000000000000000000000000000000000000b2", "chains",
    )
    sent = json.loads(base64.b64decode(fake.requests[1].headers["payment-signature"]))
    assert sent["x402Version"] == 2
    assert sent["accepted"] == offer("eip155:84532")
    assert sent["payload"] == {"signature": "0xsig", "authorization": {"from": "0xp"}}
    assert sent["extensions"]["payment-identifier"]["info"]["id"] == ctx.payment_id
    assert receipt is not None and receipt.success and receipt.transaction == "0xtx"


def test_async_payer_coroutine(serve: Any) -> None:
    fake = serve(paid_once)

    class AsyncPayer:
        networks = ["eip155:84532"]

        async def sign(self, context: PaymentContext) -> PaymentSignature:
            await asyncio.sleep(0)
            return PaymentSignature(payload={"signature": "0x"})

    async def run() -> None:
        async with AsyncAveeClient(fake.base_url, payer=AsyncPayer()) as c:
            await c.chains()

    asyncio.run(run())
    assert len(fake.requests) == 2


def test_challenge_in_the_body_and_a_finished_header(serve: Any) -> None:
    def handler(req: Any, n: int) -> Reply:
        if req.headers.get("payment-signature") != "ready-made":
            return paywall(challenge(offer("solana:mainnet")), header=False)
        return Reply(200, instance(SCHEMAS["Config"], "required"))

    fake = serve(handler)
    with AveeClient(fake.base_url, payer=Recorder([], PaymentSignature(header="ready-made"))) as c:
        c.config()
    assert len(fake.requests) == 2


@pytest.mark.parametrize("status", [402, 503])
def test_never_pays_twice_nor_retries_a_paid_request(serve: Any, status: int) -> None:
    def handler(req: Any, n: int) -> Reply:
        if "payment-signature" not in req.headers or status == 402:
            return paywall(challenge(offer("eip155:84532")))
        return problem(status, "upstream_unavailable", "later")

    fake = serve(handler)
    with AveeClient(fake.base_url, payer=Recorder(["eip155:84532"], PaymentSignature(payload={"s": 1}))) as c, pytest.raises(AveeAPIError) as info:
        c.chains()
    assert info.value.paid and info.value.status == status and len(fake.requests) == 2
    if status == 402:
        assert info.value.payment_required is not None and len(info.value.payment_required["accepts"]) == 1


def test_declines_and_mismatches_send_nothing(serve: Any) -> None:
    fake = serve(lambda req, n: paywall(challenge(offer("eip155:8453"))))
    with AveeClient(fake.base_url, payer=Recorder([], None)) as c, pytest.raises(AveePaymentError, match="declined"):
        c.chains()
    elsewhere = Recorder(["eip155:1"], PaymentSignature(payload={}))
    with AveeClient(fake.base_url, payer=elsewhere) as c, pytest.raises(AveePaymentError, match="no accepted network"):
        c.chains()
    assert elsewhere.seen == [] and len(fake.requests) == 2


def test_async_payer_on_the_sync_client_is_refused(serve: Any) -> None:
    fake = serve(lambda req, n: paywall(challenge(offer("eip155:8453"))))

    class AsyncPayer:
        networks: list[str] = []

        async def sign(self, context: PaymentContext) -> PaymentSignature:
            return PaymentSignature(payload={})

    with AveeClient(fake.base_url, payer=AsyncPayer()) as c, pytest.raises(AveePaymentError, match="AsyncAveeClient"):
        c.chains()


def test_without_a_payer_a_429_is_a_429(serve: Any) -> None:
    fake = serve(lambda req, n: problem(429, "rate_limited", "spent", {"payment-required": b64(challenge(offer("eip155:84532")))}))
    with AveeClient(fake.base_url, max_retries=0) as c, pytest.raises(AveeAPIError) as info:
        c.chains()
    assert info.value.status == 429 and not info.value.paid and info.value.payment_required is None
    assert "accept-payment" not in fake.requests[0].headers
