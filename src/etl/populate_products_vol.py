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
PRODUCTS_VOL_CSV = DATA_DIR / "products_vol.csv"
BATCH_SIZE = 5000


async def insert_in_batches(
	conn: asyncpg.Connection, query: str, rows: list[tuple]
) -> None:
	for i in range(0, len(rows), BATCH_SIZE):
		batch = rows[i : i + BATCH_SIZE]
		await conn.executemany(query, batch)
		logger.info("Inserted batch", start=i, size=len(batch))


async def populate_products_vol(pool: asyncpg.Pool) -> None:
	rows = []
	with PRODUCTS_VOL_CSV.open(encoding="cp1251") as f:
		reader = csv.DictReader(f)
		for row in reader:
			rows.append((row["Товар"], float(row["ОбъемЕд"])))

	async with pool.acquire() as conn:
		await conn.execute("TRUNCATE logistics.products_vol RESTART IDENTITY CASCADE")
		query = """
			INSERT INTO logistics.products_vol (product_id, volume_per_unit)
			VALUES ($1, $2)
		"""
		await insert_in_batches(conn, query, rows)
		logger.info("All product volumes inserted", total=len(rows))


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	await populate_products_vol(pool)


if __name__ == "__main__":
	asyncio.run(main())
