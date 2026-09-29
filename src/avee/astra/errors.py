from __future__ import annotations

from dataclasses import dataclass

RETRYABLE_STATUSES = frozenset({408, 429, 502, 503, 504})


def clip(text: str, limit: int = 200) -> str:
    return text if len(text) <= limit else text[:limit] + "…"


@dataclass(frozen=True, slots=True)
class Problem:
    type: str
    title: str
    status: int
    detail: str


class AstraError(Exception):
    pass


class AstraHTTPError(AstraError):
    def __init__(
        self,
        status: int,
        url: str,
        body: str,
        *,
        detail: str = "",
        problem: Problem | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(f"Astra answered {status} for {url}" + (f": {detail}" if detail else ""))
        self.status = status
        self.url = url
        self.body = body
        self.problem = problem
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        return self.status in RETRYABLE_STATUSES


class AstraTimeoutError(AstraError):
    pass


class AstraConnectionError(AstraError):
    pass


class AstraValidationError(AstraError, ValueError):
    pass


class AstraSubscriptionError(AstraError):
    pass
