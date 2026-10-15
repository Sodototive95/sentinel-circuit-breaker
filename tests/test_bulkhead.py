"""Unit tests for Bulkhead concurrency isolation."""

import asyncio
import unittest
from circuit_breaker.bulkhead import Bulkhead
from circuit_breaker.states import BulkheadFullError


class TestBulkhead(unittest.TestCase):
    def test_sync_bulkhead_limit(self) -> None:
        bulkhead = Bulkhead("db-pool", max_concurrent=2)
        with bulkhead:
            self.assertEqual(bulkhead.active_count, 1)
            with bulkhead:
                self.assertEqual(bulkhead.active_count, 2)
                # Saturated
                with self.assertRaises(BulkheadFullError):
                    with bulkhead:
                        pass
        self.assertEqual(bulkhead.active_count, 0)

    def test_async_bulkhead_concurrency(self) -> None:
        async def run_test() -> None:
            bulkhead = Bulkhead("async-pool", max_concurrent=2)
            async with bulkhead:
                self.assertEqual(bulkhead.active_count, 1)
                async with bulkhead:
                    self.assertEqual(bulkhead.active_count, 2)
                    with self.assertRaises(BulkheadFullError):
                        async with bulkhead:
                            pass
            self.assertEqual(bulkhead.active_count, 0)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
