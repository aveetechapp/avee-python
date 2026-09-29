from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._types import PaymentReceipt, RateLimit


def clip(text: str, limit: int = 200) -> str:
    return text if len(text) <= limit else text[:limit] + "…"


def retryable_status(status: int) -> bool:
    return status in (408, 429) or status >= 500


class AveeError(Exception):
    """Base of every error this package raises."""


class AveeAPIError(AveeError):
    """A non-2xx answer; ``code``, ``detail``, ``param`` and ``request_id`` come from the problem+json body."""

    def __init__(
        self,
        *,
        status: int,
        operation: str,
        method: str,
        url: str,
        body: str,
        headers: Mapping[str, str],
        problem: Mapping[str, Any] | None,
        rate_limit: RateLimit,
        retry_after: float | None,
        payment_required: Mapping[str, Any] | None,
        payment: PaymentReceipt | None,
        paid: bool,
    ) -> None:
        p = problem or {}
        code = p.get("code") if isinstance(p.get("code"), str) else None
        detail = clip((p.get("detail") or p.get("title") or "") if problem else body.strip())
        request_id = p.get("request_id") or headers.get("x-request-id")
        msg = f"{method} {url} answered {status}" + (f" {code}" if code else "") + (f": {detail}" if detail else "")
        super().__init__(msg + (f" (request {request_id})" if request_id else ""))
        self.status = status
        self.code: str | None = code
        self.detail: str | None = detail if isinstance(detail, str) and detail else None
        self.param: str | None = p.get("param") if isinstance(p.get("param"), str) else None
        self.request_id: str | None = request_id if isinstance(request_id, str) else None
        self.operation = operation
        self.method = method
        self.url = url
        self.body = body
        self.headers = headers
        self.problem = problem
        self.rate_limit = rate_limit
        self.retry_after = retry_after
        self.payment_required = payment_required
        self.payment = payment
        self.paid = paid

    @property
    def retryable(self) -> bool:
        return retryable_status(self.status)


class AveeTimeoutError(AveeError):
    pass


class AveeConnectionError(AveeError):
    pass


class AveeValidationError(AveeError, ValueError):
    """Bad input, caught before any request, or a response that breaks the contract."""


class AveePaymentError(AveeError):
    """The x402 flow stopped before a paid request was sent."""

    def __init__(self, message: str, operation: str, payment_required: Mapping[str, Any] | None = None) -> None:
        super().__init__(f"payment for {operation}: {message}")
        self.operation = operation
        self.payment_required = payment_required
