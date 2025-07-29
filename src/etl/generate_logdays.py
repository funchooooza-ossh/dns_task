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
		batch = rows[i : i + BATCH_SIZE]
		await conn.executemany(query, batch)
		logger.info("Inserted logdays batch", start=i, size=len(batch))


async def generate_logdays(conn: asyncpg.Connection) -> None:
	logger.info("Generating logdays...")

	rows = await conn.fetch("""
		SELECT DISTINCT b.branch_id, p.category_id
		FROM logistics.branch_product_history b
		JOIN logistics.products p ON b.product_id = p.product_id
	""")

	values = [
		(r["branch_id"], r["category_id"], random.choice([7, 14, 21]))  # noqa: S311
		for r in rows
	]

	await conn.execute("TRUNCATE logistics.logdays RESTART IDENTITY CASCADE")
	await insert_in_batches(
		conn,
		"""
		INSERT INTO logistics.logdays (branch_id, category_id, logdays)
		VALUES ($1, $2, $3)
		""",
		values,
	)

	logger.info("Inserted all logdays", total=len(values))


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	async with pool.acquire() as conn:
		await generate_logdays(conn)


if __name__ == "__main__":
	asyncio.run(main())
