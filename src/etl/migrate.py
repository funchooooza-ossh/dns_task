import structlog

from config import cfg
from core.factories.db import acquire_connection, create_pool
from migrations.queries import MIGRATIONS

logger = structlog.get_logger()


async def migrate() -> None:
	pool = await create_pool(cfg.pg_url)

	async with acquire_connection(pool) as conn:
		# Обеспечим таблицу логов
		await conn.execute("""
            CREATE TABLE IF NOT EXISTS public.migration_log (
                id SERIAL PRIMARY KEY,
                migration_name TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

		# Получим список уже применённых
		applied = await conn.fetch("SELECT migration_name FROM public.migration_log")
		applied_names = {row["migration_name"] for row in applied}

		for name, sql in MIGRATIONS.items():
			if name in applied_names:
				continue

			logger.info(f"Applying migration: {name}", stage="migrate")
			await conn.execute(sql)
			await conn.execute(
				"INSERT INTO public.migration_log (migration_name) VALUES ($1)", name
			)
		logger.info("All migrations applied.", stage="migrate")
