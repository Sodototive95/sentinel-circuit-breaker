"""Unit tests for asynchronous CircuitBreaker concurrency and thread safety."""

import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState


class TestAsyncCircuitBreaker(unittest.TestCase):
    def test_async_decorator_success(self) -> None:
        async def run_test() -> None:
            cb = CircuitBreaker("async-service", min_calls_threshold=2)

            @cb
            async def fetch_data(item_id: int) -> dict:
                await asyncio.sleep(0.005)
                return {"id": item_id, "status": "ok"}

            results = await asyncio.gather(*(fetch_data(i) for i in range(10)))
            self.assertEqual(len(results), 10)
            self.assertEqual(cb.state, CircuitState.CLOSED)
            metrics = cb.get_metrics()
            self.assertEqual(metrics.successful_calls, 10)

        asyncio.run(run_test())

    def test_async_concurrent_trip_and_rejection(self) -> None:
        async def run_test() -> None:
            cb = CircuitBreaker("flaky-service", min_calls_threshold=5, failure_rate_threshold=0.5)

            @cb
            async def failing_worker() -> None:
                await asyncio.sleep(0.005)
                raise ConnectionResetError("reset by peer")

            results = await asyncio.gather(*(failing_worker() for _ in range(10)), return_exceptions=True)
            failures = sum(1 for r in results if isinstance(r, ConnectionResetError))
            circuit_open_rejections = sum(1 for r in results if isinstance(r, CircuitOpenError))

            self.assertEqual(cb.state, CircuitState.OPEN)
            self.assertTrue(failures >= 5)
            self.assertTrue(failures + circuit_open_rejections == 10)

        asyncio.run(run_test())

    def test_multithreaded_concurrency_stress(self) -> None:
        cb = CircuitBreaker("threaded-service", min_calls_threshold=50)

        @cb
        def thread_task(x: int) -> int:
            return x * 2

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(thread_task, i) for i in range(100)]
            results = [f.result() for f in futures]

        self.assertEqual(len(results), 100)
        self.assertEqual(cb.state, CircuitState.CLOSED)
        metrics = cb.get_metrics()
        self.assertEqual(metrics.successful_calls, 100)


if __name__ == "__main__":
    unittest.main()
