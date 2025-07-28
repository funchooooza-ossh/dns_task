from typing import Annotated

import asyncpg
from fastapi import Depends, Request


async def get_pool(request: Request) -> asyncpg.Pool:
	pool: asyncpg.Pool = request.app.state.pg_pool
	return pool


Pool = Annotated["asyncpg.Pool", Depends(get_pool)]
