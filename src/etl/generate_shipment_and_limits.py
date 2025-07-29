import asyncio
import random

import asyncpg
import structlog

from config import cfg
from core.factories.db import create_pool
from logger import setup_logger

setup_logger()
logger = structlog.get_logger()

BATCH_SIZE = 5000


async def insert_in_batches(
	conn: asyncpg.Connection, query: str, rows: list[tuple]
) -> None:
	for i in range(0, len(rows), BATCH_SIZE):
		await conn.executemany(query, rows[i : i + BATCH_SIZE])
		logger.info("Batch inserted", size=len(rows[i : i + BATCH_SIZE]))


async def populate_min_shipment(conn: asyncpg.Connection) -> None:
	logger.info("Populating min_shipment...")
	rows = await conn.fetch("""
		SELECT branch_id, product_id
		FROM logistics.needs
	""")

	values = [
		(r["branch_id"], r["product_id"], random.randint(1, 5))  # noqa: S311
		for r in rows
	]

	await conn.execute("TRUNCATE logistics.min_shipment RESTART IDENTITY CASCADE")
	await insert_in_batches(
		conn,
		"""
		INSERT INTO logistics.min_shipment (branch_id, product_id, min_qty)
		VALUES ($1, $2, $3)
		""",
		values,
	)


async def populate_storage_limits(conn: asyncpg.Connection) -> None:
	logger.info("Populating storage_limits...")
	rows = await conn.fetch("""
		SELECT DISTINCT branch_id
		FROM logistics.branch_product_history
	""")

	values = [
		(r["branch_id"], random.randint(300, 1000))  # noqa: S311
		for r in rows
	]

	await conn.execute("TRUNCATE logistics.storage_limits RESTART IDENTITY CASCADE")
	await insert_in_batches(
		conn,
		"""
		INSERT INTO logistics.storage_limits (branch_id, max_volume)
		VALUES ($1, $2)
		""",
		values,
	)


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	async with pool.acquire() as conn:
		await populate_min_shipment(conn)
		await populate_storage_limits(conn)


if __name__ == "__main__":
	asyncio.run(main())
