import asyncio
from collections import defaultdict
from random import uniform
from statistics import median

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
		logger.info("Inserted batch", start=i, size=len(batch))


async def generate_needs(conn: asyncpg.Connection) -> None:
	logger.info("Generating needs...")

	rows = await conn.fetch("""
		SELECT branch_id, product_id, stock
		FROM logistics.branch_product_history
	""")

	grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
	for row in rows:
		key = (row["branch_id"], row["product_id"])
		grouped[key].append(float(row["stock"]))

	values = [
		(
			branch_id,
			product_id,
			round(
				median(stocks) * uniform(1.2, 2.0),  # noqa: S311
				2,
			),  # специально увеличиваем потребность, иначе все отфильтруется
		)
		for (branch_id, product_id), stocks in grouped.items()
	]

	await conn.execute("TRUNCATE logistics.needs RESTART IDENTITY CASCADE")
	await insert_in_batches(
		conn,
		"""
		INSERT INTO logistics.needs (branch_id, product_id, needs)
		VALUES ($1, $2, $3)
		""",
		values,
	)

	logger.info("Inserted needs", total=len(values))


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	async with pool.acquire() as conn:
		await generate_needs(conn)


if __name__ == "__main__":
	asyncio.run(main())
