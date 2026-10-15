"""Sliding window metrics accumulators for circuit health assessment."""

from __future__ import annotations

import collections
import threading
import time
from dataclasses import dataclass
from typing import Deque


@dataclass(frozen=True)
class ExecutionSample:
    """A single execution record within the sliding window."""

    timestamp: float
    duration_ms: float
    is_success: bool
    is_slow: bool


@dataclass(frozen=True)
class WindowSnapshot:
    """Immutable snapshot of current window metrics."""

    total_calls: int
    successful_calls: int
    failed_calls: int
    slow_calls: int
    failure_rate: float
    slow_call_rate: float
    average_duration_ms: float


class SlidingWindow:
    """Sliding window failure accumulator.

    Supports both count-based (ring buffer) and time-based (duration window) eviction.
    """

    def __init__(
        self,
        window_size: int = 100,
        time_window_seconds: float = 60.0,
        slow_call_threshold_ms: float = 1000.0,
    ) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        if time_window_seconds <= 0:
            raise ValueError("time_window_seconds must be > 0")

        self.window_size = window_size
        self.time_window_seconds = time_window_seconds
        self.slow_call_threshold_ms = slow_call_threshold_ms
        self._samples: Deque[ExecutionSample] = collections.deque(maxlen=window_size)
        self._lock = threading.Lock()

    def record(self, duration_ms: float, is_success: bool) -> None:
        """Record an execution outcome into the sliding window."""
        now = time.time()
        is_slow = duration_ms >= self.slow_call_threshold_ms
        sample = ExecutionSample(
            timestamp=now,
            duration_ms=duration_ms,
            is_success=is_success,
            is_slow=is_slow,
        )
        with self._lock:
            self._samples.append(sample)
            self._prune_expired(now)

    def _prune_expired(self, now: float) -> None:
        """Evict samples older than time_window_seconds."""
        cutoff = now - self.time_window_seconds
        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    def snapshot(self) -> WindowSnapshot:
        """Compute an atomic point-in-time snapshot of window metrics."""
        now = time.time()
        with self._lock:
            self._prune_expired(now)
            samples = list(self._samples)

        total = len(samples)
        if total == 0:
            return WindowSnapshot(
                total_calls=0,
                successful_calls=0,
                failed_calls=0,
                slow_calls=0,
                failure_rate=0.0,
                slow_call_rate=0.0,
                average_duration_ms=0.0,
            )

        successes = sum(1 for s in samples if s.is_success)
        failures = total - successes
        slow_calls = sum(1 for s in samples if s.is_slow)
        avg_dur = sum(s.duration_ms for s in samples) / total

        return WindowSnapshot(
            total_calls=total,
            successful_calls=successes,
            failed_calls=failures,
            slow_calls=slow_calls,
            failure_rate=failures / total,
            slow_call_rate=slow_calls / total,
            average_duration_ms=avg_dur,
        )

    def reset(self) -> None:
        """Clear all metrics from the window."""
        with self._lock:
            self._samples.clear()
