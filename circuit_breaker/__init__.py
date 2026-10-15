"""Sentinel Circuit Breaker - Resilient Fault-Tolerance & Concurrency Mesh."""

from .backoff import BackoffCalculator, JitterStrategy
from .bulkhead import Bulkhead
from .core import CircuitBreaker
from .events import (
    CallFailureEvent,
    CallSuccessEvent,
    CircuitEvent,
    CircuitRejectedEvent,
    StateChangeEvent,
)
from .states import BulkheadFullError, CircuitBreakerError, CircuitOpenError, CircuitState
from .window import SlidingWindow, WindowSnapshot

__version__ = "1.0.0"

__all__ = [
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerError",
    "CircuitOpenError",
    "BulkheadFullError",
    "Bulkhead",
    "BackoffCalculator",
    "JitterStrategy",
    "SlidingWindow",
    "WindowSnapshot",
    "CircuitEvent",
    "StateChangeEvent",
    "CallSuccessEvent",
    "CallFailureEvent",
    "CircuitRejectedEvent",
]
