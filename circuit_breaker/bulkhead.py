"""Bulkhead concurrency limiter for resource isolation."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from .states import BulkheadFullError


class Bulkhead:
    """Thread-safe and asyncio-compatible bulkhead concurrency isolation guard."""

    def __init__(self, name: str, max_concurrent: int = 10, max_queue: int = 0) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")

        self.name = name
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue

        self._active_count = 0
        self._queued_count = 0
        self._lock = threading.Lock()
        self._async_sem: asyncio.Semaphore | None = None

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active_count

    def acquire_sync(self) -> None:
        """Acquire a slot synchronously or raise BulkheadFullError."""
        with self._lock:
            if self._active_count < self.max_concurrent:
                self._active_count += 1
                return
            raise BulkheadFullError(self.name, self.max_concurrent)

    def release_sync(self) -> None:
        """Release an acquired sync slot."""
        with self._lock:
            if self._active_count > 0:
                self._active_count -= 1

    def __enter__(self) -> Bulkhead:
        self.acquire_sync()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release_sync()

    async def acquire_async(self) -> None:
        """Acquire a slot asynchronously or raise BulkheadFullError."""
        # Check thread-safe lock for state consistency
        with self._lock:
            if self._active_count < self.max_concurrent:
                self._active_count += 1
                return
            raise BulkheadFullError(self.name, self.max_concurrent)

    async def release_async(self) -> None:
        """Release an acquired async slot."""
        self.release_sync()

    async def __aenter__(self) -> Bulkhead:
        await self.acquire_async()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.release_async()
