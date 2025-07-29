import asyncio
import csv
import datetime
import itertools
import random
from pathlib import Path

import asyncpg
import structlog

from config import cfg
from core.factories.db import create_pool
from logger import setup_logger

setup_logger()
logger = structlog.get_logger()

DATA_DIR = Path(__file__).parent.parent.parent / "data"
BATCH_SIZE = 5000
MAX_BRANCH_ROWS = 500
MAX_RC_ROWS = 300
DAYS_BACK = 10


def perturb(value: float, delta: float = 0.1) -> float:
	factor = 1 + random.uniform(-delta, delta)  # noqa: S311
	return round(value * factor, 2)


async def insert_in_batches(
	conn: asyncpg.Connection, query: str, data_batch: list[tuple]
) -> None:
	await conn.executemany(query, data_batch)
	logger.info("Batch inserted", size=len(data_batch))


async def populate_history(pool: asyncpg.Pool) -> None:
	async with pool.acquire() as conn:
		await conn.execute(
			"TRUNCATE logistics.branch_product_history RESTART IDENTITY CASCADE"
		)
		await conn.execute(
			"TRUNCATE logistics.rc_product_history RESTART IDENTITY CASCADE"
		)

		with (DATA_DIR / "branch_products.csv").open(encoding="cp1251") as f:
			branch_rows = list(itertools.islice(csv.DictReader(f), MAX_BRANCH_ROWS))

		with (DATA_DIR / "rc_products.csv").open(encoding="cp1251") as f:
			rc_rows = list(itertools.islice(csv.DictReader(f), MAX_RC_ROWS))

		insert_branch = """
			INSERT INTO logistics.branch_product_history
			(date, branch_id, product_id, stock, reserved, in_transit)
			VALUES ($1, $2, $3, $4, $5, $6)
		"""
		insert_rc = """
			INSERT INTO logistics.rc_product_history
			(date, product_id, stock, reserved, in_transit)
			VALUES ($1, $2, $3, $4, $5)
		"""

		today = datetime.date.today()

		for day_offset in range(DAYS_BACK):
			date = today - datetime.timedelta(days=day_offset)

			branch_batch = []
			rc_batch = []

			for row in branch_rows:
				branch_batch.append(
					(
						date,
						row["Фирма"],
						row["Товар"],
						perturb(float(row["Остаток"])),
						perturb(float(row["Резерв"])),
						perturb(float(row["Транзит"])),
					)
				)
				if len(branch_batch) >= BATCH_SIZE:
					await insert_in_batches(conn, insert_branch, branch_batch)
					branch_batch.clear()

			if branch_batch:
				await insert_in_batches(conn, insert_branch, branch_batch)

			for row in rc_rows:
				rc_batch.append(
					(
						date,
						row["Товар"],
						perturb(float(row["Остаток"])),
						perturb(float(row["Резерв"])),
						perturb(float(row["Транзит"])),
					)
				)
				if len(rc_batch) >= BATCH_SIZE:
					await insert_in_batches(conn, insert_rc, rc_batch)
					rc_batch.clear()

			if rc_batch:
				await insert_in_batches(conn, insert_rc, rc_batch)

			logger.info("Inserted all rows for date", date=str(date))


async def main() -> None:
	pool = await create_pool(cfg.pg_url)
	await populate_history(pool)
	logger.info("History population complete.")


if __name__ == "__main__":
	asyncio.run(main())
