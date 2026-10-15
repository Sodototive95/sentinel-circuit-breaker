"""Unit tests for CircuitBreaker state management and execution flows."""

import time
import unittest
from circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    StateChangeEvent,
)


class TestCircuitBreaker(unittest.TestCase):
    def test_initial_state_is_closed(self) -> None:
        cb = CircuitBreaker("test-service")
        self.assertEqual(cb.state, CircuitState.CLOSED)

    def test_successful_executions_keep_circuit_closed(self) -> None:
        cb = CircuitBreaker("test-service", min_calls_threshold=3)

        @cb
        def ok_call() -> str:
            return "success"

        for _ in range(10):
            res = ok_call()
            self.assertEqual(res, "success")

        self.assertEqual(cb.state, CircuitState.CLOSED)
        metrics = cb.get_metrics()
        self.assertEqual(metrics.successful_calls, 10)
        self.assertEqual(metrics.failed_calls, 0)
        self.assertEqual(metrics.failure_rate, 0.0)

    def test_circuit_trips_open_when_failure_threshold_exceeded(self) -> None:
        events: list[StateChangeEvent] = []
        cb = CircuitBreaker(
            "flaky-api",
            failure_rate_threshold=0.5,
            min_calls_threshold=4,
            recovery_timeout_seconds=0.2,
        )
        cb.add_listener(lambda e: events.append(e) if isinstance(e, StateChangeEvent) else None)

        @cb
        def failing_call() -> None:
            raise ConnectionError("connection refused")

        # 4 consecutive failures >= min_calls_threshold (4) and failure_rate 100% >= 50%
        for _ in range(4):
            with self.assertRaises(ConnectionError):
                failing_call()

        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertTrue(len(events) >= 1)
        self.assertEqual(events[0].old_state, CircuitState.CLOSED)
        self.assertEqual(events[0].new_state, CircuitState.OPEN)

        # Subsequent call immediately fast-fails with CircuitOpenError
        with self.assertRaises(CircuitOpenError):
            failing_call()

    def test_circuit_half_open_recovery(self) -> None:
        cb = CircuitBreaker(
            "recovery-service",
            failure_rate_threshold=0.5,
            min_calls_threshold=2,
            recovery_timeout_seconds=0.05,
            half_open_max_calls=2,
        )

        @cb
        def dynamic_call(should_fail: bool) -> str:
            if should_fail:
                raise TimeoutError("timeout")
            return "recovered"

        # Trip to OPEN
        for _ in range(2):
            with self.assertRaises(TimeoutError):
                dynamic_call(True)

        self.assertEqual(cb.state, CircuitState.OPEN)

        # Wait for recovery timeout
        time.sleep(0.08)

        # State should evaluate to HALF_OPEN
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

        # First trial success
        self.assertEqual(dynamic_call(False), "recovered")
        # Second trial success -> Should close circuit
        self.assertEqual(dynamic_call(False), "recovered")

        self.assertEqual(cb.state, CircuitState.CLOSED)

    def test_ignored_exceptions_do_not_trip_circuit(self) -> None:
        cb = CircuitBreaker(
            "validated-service",
            min_calls_threshold=3,
            ignored_exceptions=[ValueError],
        )

        @cb
        def validated_call() -> None:
            raise ValueError("bad parameter")

        for _ in range(5):
            with self.assertRaises(ValueError):
                validated_call()

        # Should still be CLOSED because ValueError was ignored
        self.assertEqual(cb.state, CircuitState.CLOSED)
        metrics = cb.get_metrics()
        self.assertEqual(metrics.total_calls, 0)

    def test_manual_reset(self) -> None:
        cb = CircuitBreaker("resettable", min_calls_threshold=1)
        with self.assertRaises(RuntimeError):
            with cb:
                raise RuntimeError("boom")

        self.assertEqual(cb.state, CircuitState.OPEN)
        cb.reset()
        self.assertEqual(cb.state, CircuitState.CLOSED)


if __name__ == "__main__":
    unittest.main()
