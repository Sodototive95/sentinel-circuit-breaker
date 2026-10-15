"""Unit tests for sliding window metrics and ring buffer eviction."""

import time
import unittest
from circuit_breaker.window import SlidingWindow


class TestSlidingWindow(unittest.TestCase):
    def test_empty_window_snapshot(self) -> None:
        window = SlidingWindow(window_size=10)
        snap = window.snapshot()
        self.assertEqual(snap.total_calls, 0)
        self.assertEqual(snap.failure_rate, 0.0)
        self.assertEqual(snap.average_duration_ms, 0.0)

    def test_window_capacity_eviction(self) -> None:
        window = SlidingWindow(window_size=5)
        # Add 5 failures
        for _ in range(5):
            window.record(10.0, is_success=False)

        snap = window.snapshot()
        self.assertEqual(snap.total_calls, 5)
        self.assertEqual(snap.failure_rate, 1.0)

        # Add 5 successes, which should evict the 5 failures from the 5-item buffer
        for _ in range(5):
            window.record(15.0, is_success=True)

        snap = window.snapshot()
        self.assertEqual(snap.total_calls, 5)
        self.assertEqual(snap.successful_calls, 5)
        self.assertEqual(snap.failure_rate, 0.0)
        self.assertEqual(snap.average_duration_ms, 15.0)

    def test_slow_call_detection(self) -> None:
        window = SlidingWindow(window_size=10, slow_call_threshold_ms=100.0)
        window.record(50.0, is_success=True)
        window.record(150.0, is_success=True)

        snap = window.snapshot()
        self.assertEqual(snap.total_calls, 2)
        self.assertEqual(snap.slow_calls, 1)
        self.assertAlmostEqual(snap.slow_call_rate, 0.5)

    def test_time_window_pruning(self) -> None:
        window = SlidingWindow(window_size=100, time_window_seconds=0.05)
        window.record(10.0, is_success=True)
        self.assertEqual(window.snapshot().total_calls, 1)

        time.sleep(0.07)
        self.assertEqual(window.snapshot().total_calls, 0)


if __name__ == "__main__":
    unittest.main()
