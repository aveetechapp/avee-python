from __future__ import annotations

import base64
import binascii
import json
import re
import secrets
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ._types import PaymentReceipt
from .errors import AveePaymentError
from .models import PaymentRequired, PaymentRequirements

X402_VERSION = 2
_MAX_SIGNATURE_HEADER = 16 * 1024


@dataclass(frozen=True, slots=True)
class PaymentContext:
    """What the payer is asked to approve: the operation and the one ``accepts`` entry chosen."""

    operation: str
    method: str
    url: str
    requirement: PaymentRequirements
    required: PaymentRequired
    payment_id: str


@dataclass(frozen=True, slots=True)
class PaymentSignature:
    """Either the scheme ``payload`` (wrapped into an x402 v2 PaymentPayload) or a finished ``header``."""

    payload: Mapping[str, Any] | None = None
    header: str | None = None


@runtime_checkable
class Payer(Protocol):
    """Signs x402 payments. ``networks`` are CAIP-2 ids, most preferred first; empty takes the first offer."""

    networks: Sequence[str]

    def sign(self, context: PaymentContext) -> PaymentSignature | None | Awaitable[PaymentSignature | None]: ...


def _b64decode(value: str) -> bytes | None:
    v = value.strip().replace("-", "+").replace("_", "/")
    try:
        return base64.b64decode(v + "=" * (-len(v) % 4), validate=True)
    except (binascii.Error, ValueError):
        return None


def _json(raw: bytes | str | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (ValueError, RecursionError):
        return None


def decode_required(header: str | None, body: bytes) -> dict[str, Any] | None:
    candidate = _json(_b64decode(header)) if header else _json(body)
    if not isinstance(candidate, dict):
        return None
    accepts = candidate.get("accepts")
    if isinstance(accepts, list) and accepts and all(isinstance(a, dict) for a in accepts):
        return candidate
    return None


def parse_receipt(headers: Mapping[str, str]) -> PaymentReceipt | None:
    value = headers.get("payment-response") or headers.get("x-payment-response")
    if not value:
        return None
    raw = _json(_b64decode(value))
    if not isinstance(raw, dict):
        return None
    return PaymentReceipt(
        success=raw.get("success") is True,
        transaction=str(raw.get("transaction", "")),
        network=str(raw.get("network", "")),
        payer=raw.get("payer") if isinstance(raw.get("payer"), str) else None,
        error_reason=raw.get("errorReason") if isinstance(raw.get("errorReason"), str) else None,
    )


_AMOUNT = re.compile(r"^[0-9]{1,78}$")


def _is_amount(value: str) -> bool:
    return bool(_AMOUNT.match(value)) and value.strip("0") != ""


def offer_problem(a: PaymentRequirements) -> str | None:
    if not (a.network and a.asset and a.pay_to):
        return "it names no network, asset or payee"
    if not _is_amount(a.amount):
        return f"amount {a.amount[:100]!r} is not a positive integer"
    if not a.max_amount_required:
        return None
    if not _is_amount(a.max_amount_required):
        return f"max_amount_required {a.max_amount_required[:100]!r} is not a positive integer"
    if int(a.amount) > int(a.max_amount_required):
        return f"amount {a.amount} exceeds maxAmountRequired {a.max_amount_required}"
    return None


def pick(accepts: Sequence[PaymentRequirements], networks: Sequence[str]) -> int:
    if not networks:
        return next((i for i, a in enumerate(accepts) if a.scheme == "exact"), -1)
    for n in networks:
        for i, a in enumerate(accepts):
            if a.scheme == "exact" and a.network.lower() == n.lower():
                return i
    return -1


def prepare(payer: Payer, operation: str, method: str, url: str, raw: dict[str, Any]) -> tuple[PaymentContext, int]:
    try:
        required = PaymentRequired._from_json(raw)
    except ValueError as err:
        raise AveePaymentError(f"the 402 challenge is unreadable: {err}", operation, raw) from None
    choice = pick(required.accepts, list(getattr(payer, "networks", None) or ()))
    if choice < 0:
        raise AveePaymentError("no accepted network the payer supports", operation, raw)
    problem = offer_problem(required.accepts[choice])
    if problem:
        raise AveePaymentError(f"the offer is malformed: {problem}", operation, raw)
    payment_id = "avee_" + secrets.token_urlsafe(18)
    return PaymentContext(operation, method, url, required.accepts[choice], required, payment_id), choice


def finish(signature: object, context: PaymentContext, choice: int, raw: dict[str, Any]) -> str:
    op = context.operation
    if signature is None:
        raise AveePaymentError("the payer declined", op, raw)
    if not isinstance(signature, PaymentSignature):
        raise AveePaymentError("the payer returned an unusable signature", op, raw)
    if signature.header is not None:
        h = signature.header
        if not isinstance(h, str) or not h or len(h) > _MAX_SIGNATURE_HEADER or "\r" in h or "\n" in h:
            raise AveePaymentError("the payer returned an unusable PAYMENT-SIGNATURE", op, raw)
        return h
    if not isinstance(signature.payload, Mapping):
        raise AveePaymentError("the payer returned no payload", op, raw)
    version = raw.get("x402Version")
    payload: dict[str, Any] = {
        "x402Version": version if isinstance(version, int) and not isinstance(version, bool) and version > 0 else X402_VERSION,
        "accepted": raw["accepts"][choice],
        "payload": dict(signature.payload),
        "extensions": {"payment-identifier": {"info": {"id": context.payment_id}}},
    }
    if raw.get("resource") is not None:
        payload["resource"] = raw["resource"]
    try:
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as err:
        raise AveePaymentError(f"the signed payload cannot be encoded: {err}", op, raw) from None
    header = base64.b64encode(encoded).decode()
    if len(header) > _MAX_SIGNATURE_HEADER:
        raise AveePaymentError("the signed payload is too large", op, raw)
    return header
