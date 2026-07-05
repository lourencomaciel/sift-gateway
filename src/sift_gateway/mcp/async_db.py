"""Async wrappers for synchronous database-bound gateway work."""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import ParamSpec, TypeVar

import anyio

P = ParamSpec("P")
T = TypeVar("T")


async def run_sync_db(
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Run synchronous DB-bound work in a worker thread."""
    return await anyio.to_thread.run_sync(partial(func, *args, **kwargs))
