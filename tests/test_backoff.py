"""Unit tests for exponential backoff and jitter algorithms."""

import unittest
from circuit_breaker.backoff import BackoffCalculator, JitterStrategy


class TestBackoffCalculator(unittest.TestCase):
    def test_validation_constraints(self) -> None:
        with self.assertRaises(ValueError):
            BackoffCalculator(base_delay=-1.0)
        with self.assertRaises(ValueError):
            BackoffCalculator(base_delay=10.0, max_delay=5.0)
        with self.assertRaises(ValueError):
            BackoffCalculator(factor=0.5)

    def test_no_jitter_exponential_growth(self) -> None:
        calc = BackoffCalculator(base_delay=1.0, max_delay=16.0, factor=2.0, strategy=JitterStrategy.NONE)
        self.assertAlmostEqual(calc.calculate(0), 1.0)
        self.assertAlmostEqual(calc.calculate(1), 2.0)
        self.assertAlmostEqual(calc.calculate(2), 4.0)
        self.assertAlmostEqual(calc.calculate(3), 8.0)
        self.assertAlmostEqual(calc.calculate(4), 16.0)
        # Clamped to max_delay
        self.assertAlmostEqual(calc.calculate(5), 16.0)

    def test_full_jitter_bounds(self) -> None:
        calc = BackoffCalculator(base_delay=1.0, max_delay=10.0, factor=2.0, strategy=JitterStrategy.FULL)
        for attempt in range(5):
            val = calc.calculate(attempt)
            ceiling = min(10.0, 1.0 * (2**attempt))
            self.assertTrue(0.0 <= val <= ceiling)

    def test_equal_jitter_bounds(self) -> None:
        calc = BackoffCalculator(base_delay=2.0, max_delay=20.0, factor=2.0, strategy=JitterStrategy.EQUAL)
        for attempt in range(4):
            val = calc.calculate(attempt)
            ceiling = min(20.0, 2.0 * (2**attempt))
            half = ceiling / 2.0
            self.assertTrue(half <= val <= ceiling)


if __name__ == "__main__":
    unittest.main()
