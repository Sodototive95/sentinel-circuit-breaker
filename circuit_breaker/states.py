"""Circuit breaker states and exceptions."""

from __future__ import annotations

from enum import Enum


class CircuitState(str, Enum):
    """The operating state of a circuit breaker."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __str__(self) -> str:
        return self.value


class CircuitBreakerError(Exception):
    """Base exception for circuit breaker faults."""


class CircuitOpenError(CircuitBreakerError):
    """Raised when an operation is rejected because the circuit is OPEN."""

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(
            f"CircuitBreaker '{name}' is OPEN. Fast-failing request. Retry after {retry_after:.2f}s."
        )


class BulkheadFullError(CircuitBreakerError):
    """Raised when an operation is rejected due to bulkhead concurrency saturation."""

    def __init__(self, name: str, max_concurrent: int) -> None:
        self.name = name
        self.max_concurrent = max_concurrent
        super().__init__(
            f"Bulkhead '{name}' is saturated. Max concurrency ({max_concurrent}) exceeded."
        )
