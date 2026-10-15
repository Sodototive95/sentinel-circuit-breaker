"""Structured lifecycle events for CircuitBreaker telemetry and metrics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable
from .states import CircuitState


@dataclass(frozen=True, kw_only=True)
class CircuitEvent:
    """Base event emitted during circuit breaker operations."""

    circuit_name: str
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True, kw_only=True)
class StateChangeEvent(CircuitEvent):
    """Emitted when circuit breaker transitions between states."""

    old_state: CircuitState
    new_state: CircuitState
    reason: str


@dataclass(frozen=True, kw_only=True)
class CallSuccessEvent(CircuitEvent):
    """Emitted upon successful completion of a protected call."""

    duration_ms: float


@dataclass(frozen=True, kw_only=True)
class CallFailureEvent(CircuitEvent):
    """Emitted when a protected call raises an exception."""

    duration_ms: float
    exception: Exception


@dataclass(frozen=True, kw_only=True)
class CircuitRejectedEvent(CircuitEvent):
    """Emitted when a call is rejected without execution because the circuit is OPEN."""

    state: CircuitState
    retry_after: float


EventListener = Callable[[CircuitEvent], Any]
