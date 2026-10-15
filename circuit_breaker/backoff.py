"""Jittered exponential backoff algorithms for distributed systems resiliency.

Implements the algorithms analyzed in AWS Architecture:
'Exponential Backoff And Jitter' by Marc Brooker.
"""

from __future__ import annotations

import random
from enum import Enum


class JitterStrategy(str, Enum):
    """Supported backoff jitter strategies."""

    NONE = "none"
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"


class BackoffCalculator:
    """Thread-safe exponential backoff calculator with configurable jitter."""

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        factor: float = 2.0,
        strategy: JitterStrategy = JitterStrategy.FULL,
    ) -> None:
        if base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if max_delay < base_delay:
            raise ValueError("max_delay cannot be smaller than base_delay")
        if factor < 1.0:
            raise ValueError("factor must be >= 1.0")

        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor
        self.strategy = strategy
        self._last_sleep = base_delay

    def calculate(self, attempt: int) -> float:
        """Calculate sleep delay in seconds for the given attempt index (0-based)."""
        attempt = max(0, attempt)
        ceiling = min(self.max_delay, self.base_delay * (self.factor**attempt))

        if self.strategy == JitterStrategy.NONE:
            return ceiling
        elif self.strategy == JitterStrategy.FULL:
            # Full jitter: Uniform distribution between 0 and exponential ceiling
            return random.uniform(0.0, ceiling)
        elif self.strategy == JitterStrategy.EQUAL:
            # Equal jitter: Half deterministic, half random jitter
            half = ceiling / 2.0
            return half + random.uniform(0.0, half)
        elif self.strategy == JitterStrategy.DECORRELATED:
            # Decorrelated jitter: Prevents synchronized retries across independent nodes
            sleep = min(self.max_delay, random.uniform(self.base_delay, self._last_sleep * 3.0))
            self._last_sleep = sleep
            return sleep
        else:
            return ceiling
