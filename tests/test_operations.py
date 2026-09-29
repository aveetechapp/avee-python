from __future__ import annotations

import asyncio
import dataclasses
import json
from typing import Any

import pytest

from avee import OPERATIONS, AsyncAveeClient, AveeClient, models

from .conftest import CALLS, SCHEMAS, SPEC, Reply, instance


def spec_operations() -> list[str]:
    return sorted(op["operationId"] for item in SPEC["paths"].values() for m, op in item.items() if m in ("get", "post"))


def test_covers_the_spec_one_to_one() -> None:
    assert sorted(o[0] for o in OPERATIONS) == spec_operations()
    assert sorted(c["operation"] for c in CALLS) == spec_operations()


def check_request(fake: Any, call: dict[str, Any]) -> None:
    assert len(fake.requests) == 1
    req = fake.requests[0]
    assert req.method == call["method"]
    assert req.path == "/api/v1" + call["path"]
    assert sorted(req.query) == sorted(tuple(q) for q in call["query"])
    if call["body"] is not None:
        assert json.loads(req.body) == call["body"]
        assert req.headers["content-type"] == "application/json"
    else:
        assert req.body == b""
    assert "accept-payment" not in req.headers
    assert "x-api-key" not in req.headers


@pytest.mark.parametrize("variant", ["required", "full", "unknown-values"])
@pytest.mark.parametrize("call", CALLS, ids=[c["operation"] for c in CALLS])
def test_sends_the_specified_request_and_decodes(serve: Any, call: dict[str, Any], variant: str) -> None:
    body = instance(SCHEMAS[call["response"]], variant)
    fake = serve(lambda req, n: Reply(200, body))
    with AveeClient(fake.base_url) as client:
        result = getattr(client, call["python"]["method"])(*call["pathArgs"], **call["python"]["kwargs"])
    assert type(result).__name__ == call["response"]
    assert isinstance(result, getattr(models, call["response"]))
    assert dataclasses.is_dataclass(result)
    check_request(fake, call)
    assert client.last_response is not None and client.last_response.operation == call["operation"]


@pytest.mark.parametrize("call", CALLS, ids=[c["operation"] for c in CALLS])
def test_async_client_sends_the_same_request(serve: Any, call: dict[str, Any]) -> None:
    body = instance(SCHEMAS[call["response"]], "full")
    fake = serve(lambda req, n: Reply(200, body))

    async def run() -> Any:
        async with AsyncAveeClient(fake.base_url) as client:
            result = await getattr(client, call["python"]["method"])(*call["pathArgs"], **call["python"]["kwargs"])
        assert asyncio.all_tasks() == {asyncio.current_task()}
        return result

    assert type(asyncio.run(run())).__name__ == call["response"]
    check_request(fake, call)


def test_unknown_union_branch_and_values_survive() -> None:
    raw = {
        "hash": "0x1", "timestamp": "t", "chain_id": 8453, "tx_type": "x_new_kind", "maker_address": "a", "from_address": "b", "to_address": "c",
        "block_info": {"seq_no": 7, "shard_id": 1, "work_chain": 0, "x_extra": True},
        "event_data": {"x_unknown_branch": 1},
        "x_future": {"a": [1]},
    }
    tx = models.Transaction._from_json(raw)
    assert tx.tx_type == "x_new_kind"
    assert isinstance(tx.block_info, models.TonTxData) and tx.block_info.seq_no == 7
    assert tx.event_data == {"x_unknown_branch": 1}


def test_contract_breaks_are_reported_with_the_field() -> None:
    with pytest.raises(ValueError, match=r"ChainRef\.chain_id must be an integer"):
        models.ChainRef._from_json({"slug": "base", "chain_id": "8453"})
    with pytest.raises(ValueError, match=r"ChainRef\.slug is missing"):
        models.ChainRef._from_json({"chain_id": 1})
