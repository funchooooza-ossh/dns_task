from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from config import cfg
from core.factories.db import create_pool

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
	logger.info("Starting app...", stage="lifespan")
	app.state.pg_pool = await create_pool(cfg.pg_url)
	logger.info("Database pool initialized", stage="lifespan")

	yield

	logger.info("Shutting down app...", stage="lifespan")
	await app.state.pg_pool.close()
	logger.info("Database pool closed", stage="lifespan")
