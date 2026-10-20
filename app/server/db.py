"""Lakebase (Postgres) async connection pool with OAuth token refresh."""
import asyncio
import time
from typing import Optional

import asyncpg

from .config import pg_conn_params, mint_lakebase_token, PG_SCHEMA

_TOKEN_TTL_SEC = 45 * 60  # refresh well before the ~60min expiry


class DatabasePool:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._token_ts: float = 0.0
        self._lock = asyncio.Lock()
        self._unavailable_reason: Optional[str] = None

    async def _create_pool(self) -> Optional[asyncpg.Pool]:
        params = pg_conn_params()
        if not params["host"] or not params["user"]:
            self._unavailable_reason = "Lakebase not configured (missing PGHOST/PGUSER)"
            return None
        token = mint_lakebase_token()
        pool = await asyncpg.create_pool(
            host=params["host"], port=params["port"], database=params["dbname"],
            user=params["user"], password=token, ssl="require",
            min_size=1, max_size=8, command_timeout=15,
            server_settings={"search_path": PG_SCHEMA},
        )
        self._token_ts = time.time()
        self._unavailable_reason = None
        return pool

    async def get_pool(self) -> Optional[asyncpg.Pool]:
        async with self._lock:
            if self._pool is not None and (time.time() - self._token_ts) > _TOKEN_TTL_SEC:
                # token likely stale -> recycle pool
                try:
                    await self._pool.close()
                except Exception:
                    pass
                self._pool = None
            if self._pool is None:
                try:
                    self._pool = await self._create_pool()
                except Exception as e:
                    self._unavailable_reason = f"{type(e).__name__}: {e}"
                    self._pool = None
            return self._pool

    @property
    def unavailable_reason(self) -> Optional[str]:
        return self._unavailable_reason

    async def fetch(self, sql: str, *args) -> list[dict]:
        pool = await self.get_pool()
        if pool is None:
            raise LakebaseUnavailable(self._unavailable_reason or "Lakebase unavailable")
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(r) for r in rows]

    async def fetchrow(self, sql: str, *args) -> Optional[dict]:
        rows = await self.fetch(sql, *args)
        return rows[0] if rows else None

    async def execute(self, sql: str, *args) -> str:
        pool = await self.get_pool()
        if pool is None:
            raise LakebaseUnavailable(self._unavailable_reason or "Lakebase unavailable")
        async with pool.acquire() as conn:
            return await conn.execute(sql, *args)


class LakebaseUnavailable(Exception):
    pass


db = DatabasePool()
