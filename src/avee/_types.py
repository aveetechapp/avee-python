from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RateLimit:
    limit: int | None = None
    remaining: int | None = None
    reset_seconds: int | None = None
    retry_after_seconds: float | None = None
    policy: str | None = None


@dataclass(frozen=True, slots=True)
class PaymentReceipt:
    success: bool
    transaction: str
    network: str
    payer: str | None = None
    error_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ResponseInfo:
    operation: str
    status: int
    request_id: str | None
    rate_limit: RateLimit
    payment: PaymentReceipt | None
