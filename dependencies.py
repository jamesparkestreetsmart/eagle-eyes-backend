# dependencies.py

import asyncpg
from typing import Annotated
from fastapi import Depends

from config.settings import get_settings, Settings


# ---------------------------------------------------------------------------
# Pool — one per process, created at startup
# ---------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
    )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError(
            "Database pool not initialized — call init_pool() at startup"
        )
    return _pool


# ---------------------------------------------------------------------------
# Typed dependency aliases — use these in routers
# ---------------------------------------------------------------------------

PoolDep     = Annotated[asyncpg.Pool, Depends(get_pool)]
SettingsDep = Annotated[Settings, Depends(get_settings)]