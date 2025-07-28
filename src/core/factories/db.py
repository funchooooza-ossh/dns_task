from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg


async def create_pool(url: str) -> asyncpg.Pool:
	pool = await asyncpg.create_pool(dsn=url)
	return pool


@asynccontextmanager
async def acquire_connection(pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection]:
	"""context func to safely work with pool"""
	conn = await pool.acquire()
	try:
		yield conn
	finally:
		await pool.release(conn)
