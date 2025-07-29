import asyncio
import csv
from pathlib import Path

import asyncpg
import structlog

from config import cfg
from core.factories.db import create_pool
from logger import setup_logger

setup_logger()
logger = structlog.get_logger()

DATA_DIR = Path(__file__).parent.parent.parent / "data"
PRODUCTS_CSV = DATA_DIR / "products.csv"
BATCH_SIZE = 5000


async def insert_in_batches(
	conn: asyncpg.Connection, query: str, rows: list[tuple]
) -> None:
	for i in range(0, len(rows), BATCH_SIZE):
		batch = rows[i : i + BATCH_SIZE]
		await conn.executemany(query, batch)
		logger.info("Inserted batch", start=i, size=len(batch))


async def populate_products(pool: asyncpg.Pool) -> None:
	rows = []
	with PRODUCTS_CSV.open(encoding="cp1251") as f:
		reader = csv.DictReader(f)
		for row in reader:
			rows.append((row["Product_ID"], row["Category_ID"]))

	async with pool.acquire() as conn:
		await conn.execute("TRUNCATE logistics.products RESTART IDENTITY CASCADE")
		query = """
			INSERT INTO logistics.products (product_id, category_id)
			VALUES ($1, $2)
		"""
		await insert_in_batches(conn, query, rows)
		logger.info("All products inserted", total=len(rows))


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	await populate_products(pool)


if __name__ == "__main__":
	asyncio.run(main())
