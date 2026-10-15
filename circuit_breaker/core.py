"""Core CircuitBreaker implementation with sync, async, and event bus capabilities."""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
import time
from typing import Any, Callable, Sequence, TypeVar, cast

from .backoff import BackoffCalculator, JitterStrategy
from .bulkhead import Bulkhead
from .events import (
    CallFailureEvent,
    CallSuccessEvent,
    CircuitEvent,
    CircuitRejectedEvent,
    EventListener,
    StateChangeEvent,
)
from .states import CircuitOpenError, CircuitState
from .window import SlidingWindow, WindowSnapshot

F = TypeVar("F", bound=Callable[..., Any])


class CircuitBreaker:
    """Thread-safe, high-performance Circuit Breaker.

    Protects services against cascading failures by tracking execution outcomes,
    evaluating sliding window error thresholds, and managing automatic recovery probes.
    """

    def __init__(
        self,
        name: str,
        failure_rate_threshold: float = 0.5,
        slow_call_rate_threshold: float = 0.8,
        slow_call_threshold_ms: float = 1000.0,
        min_calls_threshold: int = 10,
        recovery_timeout_seconds: float = 10.0,
        half_open_max_calls: int = 3,
        expected_exceptions: Sequence[type[Exception]] | None = None,
        ignored_exceptions: Sequence[type[Exception]] | None = None,
        backoff_strategy: JitterStrategy = JitterStrategy.FULL,
        max_backoff_seconds: float = 120.0,
        bulkhead: Bulkhead | None = None,
    ) -> None:
        if not (0.0 < failure_rate_threshold <= 1.0):
            raise ValueError("failure_rate_threshold must be between 0.0 and 1.0")
        if min_calls_threshold < 1:
            raise ValueError("min_calls_threshold must be >= 1")
        if recovery_timeout_seconds <= 0:
            raise ValueError("recovery_timeout_seconds must be > 0")

        self.name = name
        self.failure_rate_threshold = failure_rate_threshold
        self.slow_call_rate_threshold = slow_call_rate_threshold
        self.slow_call_threshold_ms = slow_call_threshold_ms
        self.min_calls_threshold = min_calls_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.half_open_max_calls = half_open_max_calls
        self.expected_exceptions = tuple(expected_exceptions or [Exception])
        self.ignored_exceptions = tuple(ignored_exceptions or ())
        self.bulkhead = bulkhead

        self._state = CircuitState.CLOSED
        self._lock = threading.RLock()
        self._window = SlidingWindow(
            window_size=max(100, min_calls_threshold * 5),
            time_window_seconds=60.0,
            slow_call_threshold_ms=slow_call_threshold_ms,
        )

        self._backoff = BackoffCalculator(
            base_delay=recovery_timeout_seconds,
            max_delay=max_backoff_seconds,
            factor=2.0,
            strategy=backoff_strategy,
        )
        self._consecutive_trips = 0
        self._last_state_change = time.time()
        self._next_probe_allowed_at = 0.0
        self._half_open_calls_in_flight = 0
        self._half_open_successes = 0

        self._listeners: list[EventListener] = []

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._evaluate_state_progression()
            return self._state

    def add_listener(self, listener: EventListener) -> None:
        """Register a callback for circuit telemetry and transition events."""
        with self._lock:
            self._listeners.append(listener)

    def _emit(self, event: CircuitEvent) -> None:
        """Dispatch event to registered listeners."""
        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                pass  # Telemetry listeners must never disrupt core execution

    def _set_state(self, new_state: CircuitState, reason: str) -> None:
        """Internal state transition."""
        old_state = self._state
        if old_state == new_state:
            return

        self._state = new_state
        self._last_state_change = time.time()
        self._emit(
            StateChangeEvent(
                circuit_name=self.name,
                old_state=old_state,
                new_state=new_state,
                reason=reason,
            )
        )

    def _evaluate_state_progression(self) -> None:
        """Transition from OPEN to HALF_OPEN when cooldown timer expires."""
        if self._state == CircuitState.OPEN:
            now = time.time()
            if now >= self._next_probe_allowed_at:
                self._half_open_calls_in_flight = 0
                self._half_open_successes = 0
                self._set_state(
                    CircuitState.HALF_OPEN,
                    f"Cooldown of {self.recovery_timeout_seconds:.1f}s expired; probing upstream health.",
                )

    def acquire_permission(self) -> None:
        """Verify if a call is permitted. Raises CircuitOpenError if blocked."""
        with self._lock:
            self._evaluate_state_progression()

            if self._state == CircuitState.OPEN:
                retry_after = max(0.0, self._next_probe_allowed_at - time.time())
                self._emit(
                    CircuitRejectedEvent(
                        circuit_name=self.name,
                        state=self._state,
                        retry_after=retry_after,
                    )
                )
                raise CircuitOpenError(self.name, retry_after)

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls_in_flight >= self.half_open_max_calls:
                    retry_after = 1.0
                    self._emit(
                        CircuitRejectedEvent(
                            circuit_name=self.name,
                            state=self._state,
                            retry_after=retry_after,
                        )
                    )
                    raise CircuitOpenError(self.name, retry_after)
                self._half_open_calls_in_flight += 1

    def record_success(self, duration_ms: float) -> None:
        """Record successful execution."""
        with self._lock:
            self._emit(CallSuccessEvent(circuit_name=self.name, duration_ms=duration_ms))

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls_in_flight = max(0, self._half_open_calls_in_flight - 1)
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_max_calls:
                    # Upstream has recovered!
                    self._consecutive_trips = 0
                    self._window.reset()
                    self._set_state(
                        CircuitState.CLOSED,
                        f"Probing succeeded ({self._half_open_successes}/{self.half_open_max_calls}). Circuit recovered.",
                    )
            elif self._state == CircuitState.CLOSED:
                self._window.record(duration_ms=duration_ms, is_success=True)

    def record_failure(self, duration_ms: float, exception: Exception) -> None:
        """Record a failed execution."""
        # Ignored exceptions bypass circuit error counters
        if isinstance(exception, self.ignored_exceptions):
            return

        with self._lock:
            self._emit(
                CallFailureEvent(
                    circuit_name=self.name,
                    duration_ms=duration_ms,
                    exception=exception,
                )
            )

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls_in_flight = max(0, self._half_open_calls_in_flight - 1)
                # Any failure in HALF_OPEN immediately trips back to OPEN
                self._consecutive_trips += 1
                delay = self._backoff.calculate(self._consecutive_trips)
                self._next_probe_allowed_at = time.time() + delay
                self._set_state(
                    CircuitState.OPEN,
                    f"Probe call failed with {type(exception).__name__}. Re-tripping with backoff {delay:.2f}s.",
                )
            elif self._state == CircuitState.CLOSED:
                self._window.record(duration_ms=duration_ms, is_success=False)
                snapshot = self._window.snapshot()

                if snapshot.total_calls >= self.min_calls_threshold:
                    if snapshot.failure_rate >= self.failure_rate_threshold:
                        self._trip_open(
                            f"Failure rate ({snapshot.failure_rate:.1%}) exceeded threshold ({self.failure_rate_threshold:.1%})."
                        )
                    elif (
                        self.slow_call_rate_threshold is not None
                        and snapshot.slow_call_rate >= self.slow_call_rate_threshold
                    ):
                        self._trip_open(
                            f"Slow call rate ({snapshot.slow_call_rate:.1%}) exceeded threshold ({self.slow_call_rate_threshold:.1%})."
                        )

    def _trip_open(self, reason: str) -> None:
        """Transition from CLOSED to OPEN with backoff scheduling."""
        delay = self._backoff.calculate(self._consecutive_trips)
        self._next_probe_allowed_at = time.time() + delay
        self._set_state(CircuitState.OPEN, f"{reason} Backoff scheduled: {delay:.2f}s.")

    def get_metrics(self) -> WindowSnapshot:
        """Obtain current window metrics."""
        return self._window.snapshot()

    def reset(self) -> None:
        """Explicitly reset circuit breaker to pristine CLOSED state."""
        with self._lock:
            self._consecutive_trips = 0
            self._window.reset()
            self._set_state(CircuitState.CLOSED, "Manual administrative reset.")

    # -------------------------------------------------------------------------
    # Decorator & Context Manager Interfaces
    # -------------------------------------------------------------------------

    def __enter__(self) -> CircuitBreaker:
        self.acquire_permission()
        if self.bulkhead:
            self.bulkhead.acquire_sync()
        self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        duration_ms = (time.perf_counter() - self._ctx_start) * 1000.0
        if self.bulkhead:
            self.bulkhead.release_sync()

        if exc_type is not None and issubclass(exc_type, self.expected_exceptions):
            self.record_failure(duration_ms, exc_val)
        else:
            self.record_success(duration_ms)

    async def __aenter__(self) -> CircuitBreaker:
        self.acquire_permission()
        if self.bulkhead:
            await self.bulkhead.acquire_async()
        self._ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        duration_ms = (time.perf_counter() - self._ctx_start) * 1000.0
        if self.bulkhead:
            await self.bulkhead.release_async()

        if exc_type is not None and issubclass(exc_type, self.expected_exceptions):
            self.record_failure(duration_ms, exc_val)
        else:
            self.record_success(duration_ms)

    def __call__(self, fn: F) -> F:
        """Decorator supporting both sync and async target callables."""
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self:
                    return await fn(*args, **kwargs)

            return cast(F, async_wrapper)
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                with self:
                    return fn(*args, **kwargs)

            return cast(F, sync_wrapper)
